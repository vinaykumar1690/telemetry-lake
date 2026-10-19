//! IR predicates to DataFusion expressions.
//!
//! Shapes are chosen so the scan can use them: a column compared with a
//! literal of the column's own type (so no cast lands on the column), plain
//! prefix `LIKE` without an escape character (iceberg-rust turns it into
//! `starts_with`), and IN lists. Later index hooks match these same shapes.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::logical_expr::{Like, Operator, binary_expr, cast, not, try_cast};
use datafusion::prelude::{Expr, lit};
use logquery::{CompareOp, Predicate, TextMatch, Value};

use crate::error::{PlanError, Result};
use crate::fields::{FieldResolver, ResolvedField};

const MAX_REGEX_LEN: usize = 1024;
const REGEX_SIZE_LIMIT: usize = 1 << 20;

pub fn lower(resolver: &FieldResolver, predicate: &Predicate) -> Result<Expr> {
    match predicate {
        Predicate::And(parts) => fold(resolver, parts, Expr::and),
        Predicate::Or(parts) => fold(resolver, parts, Expr::or),
        // NULL (an absent field) counts as false, so NOT selects exactly the other rows
        Predicate::Not(inner) => Ok(not(coalesce(vec![lower(resolver, inner)?, lit(false)]))),
        Predicate::Compare { field, op, value } => compare(&resolver.resolve(field)?, *op, value, field),
        Predicate::In { field, values } => {
            let resolved = resolver.resolve(field)?;
            let list = values.iter().map(|v| literal_for(&resolved, v, field)).collect::<Result<Vec<_>>>()?;
            Ok(resolved.expr.in_list(list, false))
        }
        Predicate::Exists { field } => Ok(resolver.resolve(field)?.expr.is_not_null()),
        Predicate::Text { field, matcher } => text(&resolver.resolve(field)?, matcher),
    }
}

fn fold(resolver: &FieldResolver, parts: &[Predicate], combine: fn(Expr, Expr) -> Expr) -> Result<Expr> {
    let mut exprs = parts.iter().map(|p| lower(resolver, p));
    let first = exprs.next().ok_or_else(|| PlanError::query("empty and/or"))??;
    exprs.try_fold(first, |acc, next| Ok(combine(acc, next?)))
}

fn compare(field: &ResolvedField, op: CompareOp, value: &Value, name: &str) -> Result<Expr> {
    let ordered = !matches!(op, CompareOp::Eq | CompareOp::Ne);
    // Attribute values are strings; numeric ordering on them compares numbers,
    // and values that are not numbers never match
    let (left, right) = if field.column.is_none() && ordered && matches!(value, Value::Int(_) | Value::Float(_)) {
        let number = match value {
            Value::Int(i) => *i as f64,
            Value::Float(f) => *f,
            _ => unreachable!(),
        };
        (try_cast(field.expr.clone(), DataType::Float64), lit(number))
    } else {
        (field.expr.clone(), literal_for(field, value, name)?)
    };
    Ok(match op {
        CompareOp::Eq => left.eq(right),
        // Absent counts as "not equal"
        CompareOp::Ne => binary_expr(left, Operator::IsDistinctFrom, right),
        CompareOp::Lt => left.lt(right),
        CompareOp::Le => left.lt_eq(right),
        CompareOp::Gt => left.gt(right),
        CompareOp::Ge => left.gt_eq(right),
    })
}

/// The value as a literal of the field's own type
fn literal_for(field: &ResolvedField, value: &Value, name: &str) -> Result<Expr> {
    if field.column.is_none() {
        return Ok(lit(value.to_attribute_string()));
    }
    let scalar = match value {
        Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
        Value::Int(i) => ScalarValue::Int64(Some(*i)),
        Value::Float(f) => ScalarValue::Float64(Some(*f)),
        Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
    };
    let target = &field.data_type;
    if matches!(target, DataType::Map(_, _) | DataType::List(_) | DataType::Struct(_)) {
        return Err(PlanError::query(format!(
            "'{name}' is a {target} column; compare one of its keys instead (e.g. '{name}.key' or 'attr.key')"
        )));
    }
    let cast = scalar.cast_to(target).ok().filter(|s| !s.is_null());
    // A float compared with an integer column must not be truncated
    let lossy = matches!(value, Value::Float(f) if f.fract() != 0.0) && target.is_integer();
    match cast {
        Some(cast) if !lossy => Ok(lit(cast)),
        _ => Err(PlanError::query(format!(
            "'{name}' is {target}; cannot compare it with {}",
            value.to_attribute_string()
        ))),
    }
}

fn text(field: &ResolvedField, matcher: &TextMatch) -> Result<Expr> {
    let expr = match field.data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => field.expr.clone(),
        DataType::Map(_, _) | DataType::List(_) | DataType::Struct(_) => {
            return Err(PlanError::query("text matching needs a string field, not a map; name a key"));
        }
        _ => cast(field.expr.clone(), DataType::Utf8),
    };
    Ok(match matcher {
        TextMatch::Contains { needle, case_insensitive } => {
            like(expr, format!("%{}%", escape_like(needle)), true, *case_insensitive)
        }
        TextMatch::Prefix(prefix) if !prefix.contains(['%', '_', '\\']) => {
            like(expr, format!("{prefix}%"), false, false)
        }
        TextMatch::Prefix(prefix) => like(expr, format!("{}%", escape_like(prefix)), true, false),
        TextMatch::Regex { pattern, case_insensitive } => {
            check_regex(pattern, *case_insensitive)?;
            let flags = case_insensitive.then(|| lit("i"));
            regexp_like(expr, lit(pattern.clone()), flags)
        }
    })
}

fn like(expr: Expr, pattern: String, escaped: bool, case_insensitive: bool) -> Expr {
    Expr::Like(Like::new(false, Box::new(expr), Box::new(lit(pattern)), escaped.then_some('\\'), case_insensitive))
}

pub(crate) fn escape_like(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if matches!(c, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Rejects invalid or oversized patterns up front (HTTP 400, not an engine error)
fn check_regex(pattern: &str, case_insensitive: bool) -> Result<()> {
    if pattern.len() > MAX_REGEX_LEN {
        return Err(PlanError::query(format!("regex is longer than {MAX_REGEX_LEN} characters")));
    }
    regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .map(|_| ())
        .map_err(|e| PlanError::query(format!("invalid regex: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::logs_schema;

    fn lowered(predicate: Predicate) -> String {
        lower(&FieldResolver::new(&logs_schema()), &predicate).unwrap().to_string()
    }

    fn cmp(field: &str, op: CompareOp, value: Value) -> Predicate {
        Predicate::Compare { field: field.into(), op, value }
    }

    #[test]
    fn literals_take_the_column_type() {
        // No cast on the column: the predicate can be pushed into the scan
        assert_eq!(lowered(cmp("severity_number", CompareOp::Ge, Value::Int(17))), "severity_number >= Int32(17)");
        assert_eq!(
            lowered(cmp("severity_number", CompareOp::Eq, Value::String("9".into()))),
            "severity_number = Int32(9)"
        );
        assert_eq!(lowered(cmp("service.name", CompareOp::Eq, Value::Int(5))), "service_name = Utf8(\"5\")");
    }

    #[test]
    fn incompatible_values_are_query_errors() {
        let resolver = FieldResolver::new(&logs_schema());
        for p in [
            cmp("severity_number", CompareOp::Eq, Value::String("high".into())),
            cmp("severity_number", CompareOp::Eq, Value::Float(1.5)),
            cmp("log_attributes", CompareOp::Eq, Value::String("x".into())),
        ] {
            assert!(matches!(lower(&resolver, &p), Err(PlanError::Query(_))), "{p:?}");
        }
    }

    #[test]
    fn attribute_comparisons() {
        assert_eq!(
            lowered(cmp("attr.code", CompareOp::Eq, Value::Int(500))),
            "get_field(log_attributes, Utf8(\"code\")) = Utf8(\"500\")"
        );
        let numeric = lowered(cmp("attr.code", CompareOp::Ge, Value::Int(500)));
        assert!(
            numeric.starts_with("TRY_CAST(get_field(log_attributes, Utf8(\"code\")) AS Float64) >= Float64(500)"),
            "{numeric}"
        );
        let lexical = lowered(cmp("attr.code", CompareOp::Ge, Value::String("b".into())));
        assert_eq!(lexical, "get_field(log_attributes, Utf8(\"code\")) >= Utf8(\"b\")");
    }

    #[test]
    fn negation_is_null_safe() {
        let ne = lowered(cmp("attr.env", CompareOp::Ne, Value::String("prod".into())));
        assert!(ne.contains("IS DISTINCT FROM"), "{ne}");
        let not = lowered(Predicate::Not(Box::new(Predicate::Exists { field: "attr.env".into() })));
        assert!(not.starts_with("NOT coalesce("), "{not}");
    }

    #[test]
    fn text_matching() {
        let contains = |needle: &str, ci| Predicate::Text {
            field: "body".into(),
            matcher: TextMatch::Contains { needle: needle.into(), case_insensitive: ci },
        };
        assert_eq!(lowered(contains("50%_off\\", false)), "body LIKE Utf8(\"%50\\%\\_off\\\\%\") ESCAPE '\\'");
        assert!(lowered(contains("x", true)).contains("ILIKE"));
        let prefix = |p: &str| Predicate::Text { field: "service.name".into(), matcher: TextMatch::Prefix(p.into()) };
        assert_eq!(lowered(prefix("check")), "service_name LIKE Utf8(\"check%\")", "plain prefix: no ESCAPE, pushable");
        assert!(lowered(prefix("a_b")).contains("ESCAPE"));
        let regex = |p: &str, ci| Predicate::Text {
            field: "body".into(),
            matcher: TextMatch::Regex { pattern: p.into(), case_insensitive: ci },
        };
        assert_eq!(lowered(regex("err(or)?", false)), "regexp_like(body, Utf8(\"err(or)?\"))");
        assert!(lowered(regex("x", true)).contains("Utf8(\"i\")"));
        // Non-string columns are cast for text matching
        let on_number = Predicate::Text { field: "severity_number".into(), matcher: TextMatch::Prefix("1".into()) };
        assert!(lowered(on_number).starts_with("CAST(severity_number AS Utf8)"));
    }

    #[test]
    fn bad_regexes_are_query_errors() {
        let resolver = FieldResolver::new(&logs_schema());
        for pattern in ["(unclosed".to_string(), "a".repeat(2000), "x{1000}{1000}".to_string()] {
            let p = Predicate::Text {
                field: "body".into(),
                matcher: TextMatch::Regex { pattern: pattern.clone(), case_insensitive: false },
            };
            assert!(matches!(lower(&resolver, &p), Err(PlanError::Query(_))), "{pattern}");
        }
    }
}
