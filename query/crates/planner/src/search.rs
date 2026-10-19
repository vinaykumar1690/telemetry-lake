//! The log search plan: time range + filters + cursor, ordered by the row
//! key, limited to one page.
//!
//! ```text
//! Projection(default fields + requested fields)
//!   Limit(limit + 1)             -- one extra row tells whether a next page exists
//!     Sort(timestamp, topic, partition, offset, record_index; DESC or ASC)
//!       Filter(time range AND filter AND after-cursor)   -- pushed into the scan
//!         TableScan(logs)
//! ```
//!
//! DataFusion turns Sort + Limit into a TopK, so a page costs one pass over
//! the files that survive pruning, not a full sort.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::{DataFrame, Expr, SessionContext, ident, lit};
use logquery::{Cursor, LogQuery, Order, RowKey};

use crate::error::{PlanError, Result};
use crate::fields::FieldResolver;
use crate::filters;
use crate::schema::{DEFAULT_FIELDS, OFFSET, PARTITION, RECORD_INDEX, ROW_KEY, TIMESTAMP, TOPIC};

pub struct SearchPlanner {
    resolver: FieldResolver,
    timezone: Option<Arc<str>>,
    defaults: Vec<String>,
}

/// One page of results
#[derive(Debug)]
pub struct SearchOutput {
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
    /// Present when more rows match
    pub next_cursor: Option<Cursor>,
}

impl SearchPlanner {
    /// Fails if the table lacks the row-key columns with the expected types
    pub fn new(schema: &Schema) -> Result<Self> {
        let expect = |name: &str, ok: fn(&DataType) -> bool, wanted: &str| -> Result<DataType> {
            let field =
                schema.field_with_name(name).map_err(|_| PlanError::Schema(format!("missing column {name}")))?;
            if !ok(field.data_type()) {
                return Err(PlanError::Schema(format!("column {name} is {}, expected {wanted}", field.data_type())));
            }
            Ok(field.data_type().clone())
        };
        let timestamp =
            expect(TIMESTAMP, |t| matches!(t, DataType::Timestamp(TimeUnit::Microsecond, _)), "timestamp(us)")?;
        expect(TOPIC, |t| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View), "string")?;
        expect(PARTITION, |t| *t == DataType::Int32, "int32")?;
        expect(OFFSET, |t| *t == DataType::Int64, "int64")?;
        expect(
            RECORD_INDEX,
            |t| *t == DataType::Int32,
            "int32 (written by appenders since the _record_index column was added)",
        )?;

        let DataType::Timestamp(_, timezone) = timestamp else { unreachable!() };
        let defaults = DEFAULT_FIELDS
            .iter()
            .filter(|name| schema.field_with_name(name).is_ok())
            .map(|name| name.to_string())
            .collect();
        Ok(Self { resolver: FieldResolver::new(schema), timezone, defaults })
    }

    pub fn resolver(&self) -> &FieldResolver {
        &self.resolver
    }

    /// Builds the page plan (limit + 1 rows) without running it
    pub fn frame(&self, ctx: &SessionContext, table: Arc<dyn TableProvider>, query: &LogQuery) -> Result<DataFrame> {
        let ts = ident(TIMESTAMP);
        let mut conditions = vec![
            ts.clone().gt_eq(self.timestamp(query.time.start_us)),
            ts.clone().lt(self.timestamp(query.time.end_us)),
        ];
        if let Some(filter) = &query.filter {
            conditions.push(filters::lower(&self.resolver, filter)?);
        }
        if let Some(cursor) = &query.cursor {
            conditions.extend(self.after(&cursor.after, query.order));
        }
        let filter = conditions.into_iter().reduce(Expr::and).expect("time conditions");

        let ascending = query.order == Order::Asc;
        let sort: Vec<SortExpr> = ROW_KEY.iter().map(|c| ident(*c).sort(ascending, false)).collect();
        let limit = query.limit.checked_add(1).ok_or_else(|| PlanError::query("limit is too large"))?;

        Ok(ctx
            .read_table(table)?
            .filter(filter)?
            .sort(sort)?
            .limit(0, Some(limit))?
            .select(self.projection(&query.fields)?)?)
    }

    /// Runs one page and computes the cursor for the next
    pub async fn run(
        &self,
        ctx: &SessionContext,
        table: Arc<dyn TableProvider>,
        query: &LogQuery,
        snapshot_id: Option<i64>,
    ) -> Result<SearchOutput> {
        let batches = self.frame(ctx, table, query)?.collect().await?;
        let mut kept = Vec::with_capacity(batches.len());
        let mut rows = 0;
        let mut more = false;
        for batch in batches {
            let room = query.limit - rows;
            if batch.num_rows() > room {
                more = true;
                if room > 0 {
                    kept.push(batch.slice(0, room));
                    rows += room;
                }
                break;
            }
            rows += batch.num_rows();
            kept.push(batch);
        }
        let next_cursor = match (more, kept.last()) {
            (true, Some(last)) => Some(Cursor::new(query, snapshot_id, row_key(last, last.num_rows() - 1)?)),
            _ => None,
        };
        Ok(SearchOutput { batches: kept, rows, next_cursor })
    }

    fn timestamp(&self, micros: i64) -> Expr {
        lit(ScalarValue::TimestampMicrosecond(Some(micros), self.timezone.clone()))
    }

    /// Rows strictly after `key` in the given order
    fn after(&self, key: &RowKey, order: Order) -> Vec<Expr> {
        let values = [
            self.timestamp(key.ts_us),
            lit(key.topic.clone()),
            lit(key.partition),
            lit(key.offset),
            lit(key.record_index),
        ];
        let beyond = |column: Expr, value: Expr| match order {
            Order::Desc => column.lt(value),
            Order::Asc => column.gt(value),
        };
        // (c0, c1, ...) beyond (v0, v1, ...), lexicographically
        let mut tuple = beyond(ident(ROW_KEY[4]), values[4].clone());
        for i in (0..4).rev() {
            tuple = beyond(ident(ROW_KEY[i]), values[i].clone()).or(ident(ROW_KEY[i]).eq(values[i].clone()).and(tuple));
        }
        // The plain time bound is redundant but prunes files and row groups
        let time_bound = match order {
            Order::Desc => ident(TIMESTAMP).lt_eq(values[0].clone()),
            Order::Asc => ident(TIMESTAMP).gt_eq(values[0].clone()),
        };
        vec![time_bound, tuple]
    }

    fn projection(&self, requested: &[String]) -> Result<Vec<Expr>> {
        let mut names: Vec<String> = self.defaults.clone();
        let mut exprs: Vec<Expr> = self.defaults.iter().map(|c| ident(c.as_str())).collect();
        for name in requested {
            let resolved = self.resolver.resolve(name)?;
            if resolved.column.as_ref().is_some_and(|c| self.defaults.contains(c)) || names.contains(name) {
                continue;
            }
            names.push(name.clone());
            exprs.push(resolved.expr.alias(name));
        }
        Ok(exprs)
    }
}

fn row_key(batch: &RecordBatch, row: usize) -> Result<RowKey> {
    let value = |name: &str| -> Result<ScalarValue> {
        let column = batch
            .column_by_name(name)
            .ok_or_else(|| PlanError::Schema(format!("result is missing key column {name}")))?;
        Ok(ScalarValue::try_from_array(column, row)?)
    };
    let missing = |name: &str| PlanError::Schema(format!("key column {name} is NULL"));
    Ok(RowKey {
        ts_us: match value(TIMESTAMP)? {
            ScalarValue::TimestampMicrosecond(Some(v), _) => v,
            _ => return Err(missing(TIMESTAMP)),
        },
        topic: match value(TOPIC)? {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) | ScalarValue::Utf8View(Some(v)) => v,
            _ => return Err(missing(TOPIC)),
        },
        partition: match value(PARTITION)? {
            ScalarValue::Int32(Some(v)) => v,
            _ => return Err(missing(PARTITION)),
        },
        offset: match value(OFFSET)? {
            ScalarValue::Int64(Some(v)) => v,
            _ => return Err(missing(OFFSET)),
        },
        record_index: match value(RECORD_INDEX)? {
            ScalarValue::Int32(Some(v)) => v,
            _ => return Err(missing(RECORD_INDEX)),
        },
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::Schema;
    use logquery::{CompareOp, Predicate, TextMatch, TimeRange, Value};

    use super::*;
    pub use crate::fixtures::{BASE_US, ROWS, logs_schema, logs_table, logs_table_with_rows};

    pub fn query(filter: Option<Predicate>) -> LogQuery {
        LogQuery {
            time: TimeRange { start_us: BASE_US, end_us: BASE_US + 3600 * 1_000_000 },
            filter,
            fields: vec!["attr.seq".into()],
            order: Order::Desc,
            limit: 100,
            cursor: None,
        }
    }

    async fn seqs(query: &LogQuery) -> (Vec<usize>, Option<Cursor>) {
        seqs_in(logs_table(), query).await
    }

    async fn seqs_in(table: Arc<dyn TableProvider>, query: &LogQuery) -> (Vec<usize>, Option<Cursor>) {
        let planner = SearchPlanner::new(&logs_schema()).unwrap();
        let out = planner.run(&SessionContext::new(), table, query, Some(7)).await.unwrap();
        let mut seqs = Vec::new();
        for batch in &out.batches {
            let column = batch.column_by_name("attr.seq").expect("requested field is projected");
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            seqs.extend(column.iter().map(|s| s.unwrap().parse::<usize>().unwrap()));
        }
        assert_eq!(seqs.len(), out.rows);
        (seqs, out.next_cursor)
    }

    /// Expected order: by (timestamp, topic, partition, offset, record_index)
    fn sorted(mut rows: Vec<usize>, order: Order) -> Vec<usize> {
        let key = |i: &usize| (*i / 4, *i % 2, *i / 6, (*i / 2) % 3);
        rows.sort_by_key(key);
        if order == Order::Desc {
            rows.reverse();
        }
        rows
    }

    fn cmp(field: &str, op: CompareOp, value: Value) -> Option<Predicate> {
        Some(Predicate::Compare { field: field.into(), op, value })
    }

    #[tokio::test]
    async fn newest_first_with_a_full_key_order() {
        let (rows, cursor) = seqs(&query(None)).await;
        assert_eq!(rows, sorted((0..ROWS).collect(), Order::Desc));
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn time_range_is_half_open() {
        let mut q = query(None);
        q.time = TimeRange { start_us: BASE_US + 1_000_000, end_us: BASE_US + 3_000_000 };
        let (rows, _) = seqs(&q).await;
        assert_eq!(rows, sorted((4..12).collect(), Order::Desc));
    }

    #[tokio::test]
    async fn pages_cover_every_row_once_across_timestamp_ties() {
        for order in [Order::Desc, Order::Asc] {
            let mut q = query(None);
            q.order = order;
            q.limit = 7;
            let mut all = Vec::new();
            loop {
                let (rows, cursor) = seqs(&q).await;
                assert!(rows.len() <= 7);
                all.extend(rows);
                match cursor {
                    Some(c) => {
                        assert_eq!(c.snapshot_id, Some(7));
                        c.check(&q).unwrap();
                        q.cursor = Some(c);
                    }
                    None => break,
                }
            }
            assert_eq!(all, sorted((0..ROWS).collect(), order), "{order:?}");
        }
    }

    /// Nessie keeps only the current snapshot, so a cursor's later pages may
    /// read a table that has grown since its first page
    #[tokio::test]
    async fn pages_stay_exact_when_rows_are_appended_between_pages() {
        for order in [Order::Desc, Order::Asc] {
            let mut q = query(None);
            q.order = order;
            q.limit = 7;
            let (mut all, mut rows_now) = (Vec::new(), ROWS);
            loop {
                let (rows, cursor) = seqs_in(logs_table_with_rows(rows_now), &q).await;
                all.extend(rows);
                rows_now += 5; // a commit lands before the next page
                match cursor {
                    Some(c) => q.cursor = Some(c),
                    None => break,
                }
            }
            let mut unique = all.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), all.len(), "{order:?}: a row was repeated");
            assert!((0..ROWS).all(|i| all.contains(&i)), "{order:?}: a row of the first page's table was skipped");
            assert_eq!(all, sorted(all.clone(), order), "{order:?}: order broke across pages");
        }
    }

    #[tokio::test]
    async fn exact_page_boundary_has_no_extra_cursor() {
        let mut q = query(None);
        q.limit = ROWS;
        let (rows, cursor) = seqs(&q).await;
        assert_eq!(rows.len(), ROWS);
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn column_and_map_tags() {
        let expect = |f: fn(usize) -> bool| sorted((0..ROWS).filter(|i| f(*i)).collect(), Order::Desc);
        let in_codes = Some(Predicate::In {
            field: "attr.http.status_code".into(),
            values: vec![Value::Int(500), Value::String("404".into())],
        });
        for (filter, expected) in [
            (cmp("service.name", CompareOp::Eq, Value::String("checkout".into())), expect(|i| i % 3 == 0)),
            (cmp("severity_number", CompareOp::Ge, Value::Int(17)), expect(|i| i % 4 >= 2)),
            (in_codes, expect(|i| i % 3 != 0)),
            (cmp("attr.http.status_code", CompareOp::Ge, Value::Int(404)), expect(|i| i % 3 != 0)),
            (cmp("resource.k8s.pod.name", CompareOp::Eq, Value::String("pod-1".into())), expect(|i| i % 2 == 1)),
            // Bare names fall back from log to resource attributes
            (cmp("k8s.pod.name", CompareOp::Eq, Value::String("pod-1".into())), expect(|i| i % 2 == 1)),
            (cmp("seq", CompareOp::Eq, Value::Int(7)), vec![7]),
        ] {
            assert_eq!(seqs(&query(filter.clone())).await.0, expected, "{filter:?}");
        }
    }

    #[tokio::test]
    async fn absent_attributes() {
        let has_user = |i: &usize| i.is_multiple_of(4);
        let all: Vec<usize> = (0..ROWS).collect();
        let exists = Predicate::Exists { field: "attr.user".into() };
        assert_eq!(
            seqs(&query(Some(exists.clone()))).await.0,
            sorted(all.iter().copied().filter(has_user).collect(), Order::Desc)
        );
        let missing = Some(Predicate::Not(Box::new(exists)));
        assert_eq!(
            seqs(&query(missing)).await.0,
            sorted(all.iter().copied().filter(|i| !has_user(i)).collect(), Order::Desc)
        );
        // != matches rows without the attribute too
        let ne = cmp("attr.user", CompareOp::Ne, Value::String("u0".into()));
        assert_eq!(seqs(&query(ne)).await.0, sorted((1..ROWS).collect(), Order::Desc));
    }

    #[tokio::test]
    async fn grep() {
        let text = |matcher| Some(Predicate::Text { field: "body".into(), matcher });
        let timeouts = sorted((0..ROWS).filter(|i| i % 5 == 0).collect(), Order::Desc);
        let contains = |needle: &str, ci| TextMatch::Contains { needle: needle.into(), case_insensitive: ci };
        assert_eq!(seqs(&query(text(contains("Timeout", false)))).await.0, timeouts);
        assert!(seqs(&query(text(contains("timeout", false)))).await.0.is_empty(), "case-sensitive by default");
        assert_eq!(seqs(&query(text(contains("TIMEOUT", true)))).await.0, timeouts);
        assert!(seqs(&query(text(contains("%", false)))).await.0.is_empty(), "wildcards are literal");
        let regex = TextMatch::Regex { pattern: r"^request 1\d ".into(), case_insensitive: false };
        assert_eq!(seqs(&query(text(regex))).await.0, sorted((10..20).collect(), Order::Desc));
    }

    #[tokio::test]
    async fn requested_fields_are_projected_by_name() {
        let planner = SearchPlanner::new(&logs_schema()).unwrap();
        let mut q = query(None);
        q.fields = vec!["k8s.pod.name".into(), "service.name".into(), "k8s.pod.name".into()];
        let df = planner.frame(&SessionContext::new(), logs_table(), &q).unwrap();
        let names: Vec<_> = df.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert!(names.contains(&"k8s.pod.name".to_string()));
        assert_eq!(names.iter().filter(|n| *n == "k8s.pod.name").count(), 1);
        assert!(!names.contains(&"service.name".to_string()), "already returned as service_name");
        assert_eq!(names.len(), DEFAULT_FIELDS.len() + 1);
    }

    #[test]
    fn rejects_tables_without_the_row_key() {
        let schema = logs_schema();
        let fields: Vec<_> = schema.fields().iter().filter(|f| f.name() != "_record_index").cloned().collect();
        let err = SearchPlanner::new(&Schema::new(fields)).err().unwrap();
        assert!(matches!(err, PlanError::Schema(ref m) if m.contains("_record_index")), "{err}");
    }
}
