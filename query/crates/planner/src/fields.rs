//! The single field namespace shared by all frontends.
//!
//! A name resolves, in order, to:
//! 1. a column with exactly that name (`service_name`, `_kafka_offset`)
//! 2. an alias (`severity`, `level` → `severity_text`; `message` → `body`; ...)
//! 3. the column named like the dotted OTel key with `.` → `_`
//!    (`service.name` → `service_name`, and any attribute later promoted to
//!    a column the same way)
//! 4. otherwise an attribute: `log_attributes[name]`, falling back to
//!    `resource_attributes[name]`
//!
//! Prefixes force a source: `attr.<key>` (log attributes), `resource.<key>`
//! (the resource column the appender extracted, else resource attributes).
//! Attribute values are strings; a missing key is NULL.

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::prelude::{Expr, ident};

use crate::error::{PlanError, Result};
use crate::schema::{LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES, RESOURCE_COLUMNS};

const ALIASES: &[(&str, &str)] = &[
    ("time", "timestamp"),
    ("@timestamp", "timestamp"),
    ("severity", "severity_text"),
    ("level", "severity_text"),
    ("message", "body"),
    ("msg", "body"),
    ("deployment.environment.name", "deployment_environment"),
];

/// A resolved field: the expression that reads it and its type
#[derive(Debug, Clone)]
pub struct ResolvedField {
    pub expr: Expr,
    pub data_type: DataType,
    /// The column it reads, when it is a plain column (stats and pushdown apply)
    pub column: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FieldResolver {
    columns: HashMap<String, DataType>,
    has_resource_map: bool,
    has_log_map: bool,
}

impl FieldResolver {
    pub fn new(schema: &Schema) -> Self {
        let columns: HashMap<_, _> =
            schema.fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect();
        let is_map = |name: &str| matches!(columns.get(name), Some(DataType::Map(_, _)));
        Self { has_resource_map: is_map(RESOURCE_ATTRIBUTES), has_log_map: is_map(LOG_ATTRIBUTES), columns }
    }

    pub fn column_type(&self, name: &str) -> Option<&DataType> {
        self.columns.get(name)
    }

    pub fn resolve(&self, name: &str) -> Result<ResolvedField> {
        let name = name.trim();
        if let Some(key) = name.strip_prefix("attr.").or_else(|| name.strip_prefix("attributes.")) {
            return self.attribute(LOG_ATTRIBUTES, self.has_log_map, key);
        }
        if let Some(key) = name.strip_prefix("resource.") {
            let underscored = key.replace('.', "_");
            if RESOURCE_COLUMNS.contains(&underscored.as_str()) && self.columns.contains_key(&underscored) {
                return Ok(self.column(&underscored));
            }
            return self.attribute(RESOURCE_ATTRIBUTES, self.has_resource_map, key);
        }

        if self.columns.contains_key(name) {
            return Ok(self.column(name));
        }
        if let Some((_, target)) = ALIASES.iter().find(|(alias, _)| *alias == name)
            && self.columns.contains_key(*target)
        {
            return Ok(self.column(target));
        }
        let underscored = name.replace('.', "_");
        if underscored != name && self.columns.contains_key(&underscored) {
            return Ok(self.column(&underscored));
        }

        match (self.has_log_map, self.has_resource_map) {
            (true, true) => Ok(ResolvedField {
                expr: coalesce(vec![
                    get_field(ident(LOG_ATTRIBUTES), name),
                    get_field(ident(RESOURCE_ATTRIBUTES), name),
                ]),
                data_type: DataType::Utf8,
                column: None,
            }),
            (true, false) => self.attribute(LOG_ATTRIBUTES, true, name),
            (false, true) => self.attribute(RESOURCE_ATTRIBUTES, true, name),
            (false, false) => Err(PlanError::query(format!("unknown field '{name}'"))),
        }
    }

    fn column(&self, name: &str) -> ResolvedField {
        ResolvedField { expr: ident(name), data_type: self.columns[name].clone(), column: Some(name.to_string()) }
    }

    fn attribute(&self, map: &str, present: bool, key: &str) -> Result<ResolvedField> {
        if key.is_empty() {
            return Err(PlanError::query(format!("missing attribute key after '{map}' prefix")));
        }
        if !present {
            return Err(PlanError::Schema(format!("table has no MAP column {map}")));
        }
        Ok(ResolvedField { expr: get_field(ident(map), key), data_type: DataType::Utf8, column: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::logs_schema;

    fn resolve(name: &str) -> ResolvedField {
        FieldResolver::new(&logs_schema()).resolve(name).unwrap()
    }

    #[test]
    fn columns_aliases_and_dotted_names() {
        assert_eq!(resolve("service_name").column.as_deref(), Some("service_name"));
        assert_eq!(resolve("service.name").column.as_deref(), Some("service_name"));
        assert_eq!(resolve("resource.service.name").column.as_deref(), Some("service_name"));
        assert_eq!(resolve("level").column.as_deref(), Some("severity_text"));
        assert_eq!(resolve("message").column.as_deref(), Some("body"));
        assert_eq!(resolve("deployment.environment.name").column.as_deref(), Some("deployment_environment"));
        assert_eq!(resolve("trace_id").data_type, DataType::Utf8);
        assert!(matches!(resolve("timestamp").data_type, DataType::Timestamp(_, _)));
    }

    #[test]
    fn attributes() {
        let bare = resolve("http.status_code");
        assert_eq!(bare.column, None);
        assert_eq!(bare.data_type, DataType::Utf8);
        assert!(bare.expr.to_string().contains("coalesce"), "{}", bare.expr);

        let log = resolve("attr.service.name");
        assert_eq!(log.column, None, "attr. never maps to a column");
        assert!(log.expr.to_string().contains("log_attributes"), "{}", log.expr);

        let resource = resolve("resource.k8s.pod.name");
        assert!(resource.expr.to_string().contains("resource_attributes"), "{}", resource.expr);
        // resource.<key> only maps to columns extracted from the resource
        assert!(resolve("resource.body").column.is_none());
    }

    #[test]
    fn empty_attribute_key_is_rejected() {
        assert!(FieldResolver::new(&logs_schema()).resolve("attr.").is_err());
    }
}
