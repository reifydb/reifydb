// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod call;
mod display;

use crate::function::math;
use base::expression::{Expression, PrefixOperator};
use base::function::FunctionRegistry;
use base::{ColumnValues, Row, RowMeta, SortDirection, Value, ValueKind};
use dataframe::DataFrame;
use rql::plan::{Plan, QueryPlan};
use std::ops::Deref;
use std::vec;
use transaction::{CatalogTx, NopStore, Rx, SchemaRx, SchemaTx, StoreRx, StoreToCreate, Tx};

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub value: ValueKind,
}

#[derive(Debug)]
pub enum ExecutionResult {
    CreateSchema { schema: String },
    CreateTable { schema: String, table: String },
    InsertIntoTable { schema: String, table: String, inserted: usize },
    Query { columns: Vec<Column>, rows: Vec<Row> },
}

impl From<DataFrame> for ExecutionResult {
    fn from(value: DataFrame) -> Self {
        let columns: Vec<Column> = value
            .columns
            .iter()
            .map(|c| {
                let value = match &c.data {
                    ColumnValues::Float8(_, _) => ValueKind::Float8,
                    ColumnValues::Int2(_, _) => ValueKind::Int2,
                    ColumnValues::Text(_, _) => ValueKind::Text,
                    ColumnValues::Bool(_, _) => ValueKind::Bool,
                    ColumnValues::Undefined(_) => ValueKind::Undefined,
                };

                Column { name: c.name.clone(), value }
            })
            .collect();

        let row_count = value.columns.first().map_or(0, |col| col.data.len());
        let mut rows = Vec::with_capacity(row_count);

        for row_idx in 0..row_count {
            let mut row = Vec::with_capacity(value.columns.len());

            for col in &value.columns {
                let value = match &col.data {
                    ColumnValues::Float8(vals, valid) => {
                        if valid[row_idx] {
                            Value::float8(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }

                    ColumnValues::Int2(vals, valid) => {
                        if valid[row_idx] {
                            Value::Int2(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Text(vals, valid) => {
                        if valid[row_idx] {
                            Value::Text(vals[row_idx].clone())
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Bool(vals, valid) => {
                        if valid[row_idx] {
                            Value::Bool(vals[row_idx])
                        } else {
                            Value::Undefined
                        }
                    }
                    ColumnValues::Undefined(_) => Value::Undefined,
                };
                row.push(value);
            }

            rows.push(row);
        }

        ExecutionResult::Query { columns, rows }
    }
}

pub struct Executor {
    pub functions: FunctionRegistry,
}

pub fn execute_plan_mut(plan: Plan, tx: &mut impl Tx) -> crate::Result<ExecutionResult> {
    Ok(match plan {
        Plan::CreateSchema { name, if_not_exists } => {
            if if_not_exists {
                tx.catalog_mut().unwrap().create_if_not_exists(&name).unwrap();
            } else {
                tx.catalog_mut().unwrap().create(&name).unwrap();
            }
            ExecutionResult::CreateSchema { schema: name }
        }
        Plan::CreateTable { schema, name, if_not_exists, columns } => {
            if if_not_exists {
                unimplemented!()
            } else {
                tx.schema_mut(&schema)
                    .unwrap()
                    .create(StoreToCreate::Table { name: name.clone(), columns });
            }

            ExecutionResult::CreateTable { schema, table: name }
        }
        Plan::InsertIntoTableValues { schema, store: name, columns, rows_to_insert } => {
            let mut values = Vec::with_capacity(rows_to_insert.len());

            for row in rows_to_insert {
                let mut row_values = Vec::with_capacity(row.len());
                for expr in row {
                    match expr {
                        Expression::Constant(value) => row_values.push(value),
                        _ => unimplemented!(),
                    }
                }
                values.push(row_values);
            }

            let result = tx.insert(name.deref(), values).unwrap();

            ExecutionResult::InsertIntoTable { schema, table: name, inserted: result.inserted }
        }
        Plan::Query(_) => execute_plan(plan, tx)?,
    })
}

pub fn execute_plan(plan: Plan, rx: &impl Rx) -> crate::Result<ExecutionResult> {
    let plan = match plan {
        Plan::Query(query) => query,
        _ => unreachable!(), // FIXME
    };
    // let (labels, iter) = execute_node(plan, rx, vec![], None, None, None)?;
    // Ok(ExecutionResult::Query { labels, rows: iter.collect() })

    Ok(crate::execute::execute(plan, rx).unwrap())
}

fn execute_node<'a>(
    node: QueryPlan,
    rx: &'a impl Rx,
    current_labels: Vec<RowMeta>,
    current_schema: Option<String>,
    current_store: Option<String>,
    input: Option<Box<dyn Iterator<Item = Vec<Value>> + 'a>>,
) -> crate::Result<(Vec<RowMeta>, Box<dyn Iterator<Item = Vec<Value>> + 'a>)> {
    let (labels, result_iter, schema, store, next): (
        Vec<RowMeta>,
        Box<dyn Iterator<Item = Vec<Value>>>,
        Option<String>,
        Option<String>,
        Option<Box<QueryPlan>>,
    ) = match node {
        QueryPlan::Scan { schema, store, next, .. } => (
            current_labels,
            Box::new(rx.scan(&store).unwrap()),
            Some(schema),
            Some(store.to_string()),
            next,
        ),

        QueryPlan::Limit { limit: count, next, .. } => {
            let input_iter = input.ok_or("Missing input for Limit").unwrap();
            (current_labels, Box::new(input_iter.take(count)), current_schema, current_store, next)
        }

        QueryPlan::Sort { keys, next } => {
            let input_iter = input.ok_or("Missing input for Sort").unwrap();

            // Collect all rows
            let mut rows: Vec<Vec<Value>> = input_iter.collect();

            // Figure out the sort indices from current_labels
            let indices: Vec<(usize, &SortDirection)> = keys
                .iter()
                .map(|key| {
                    let idx = current_labels
                        .iter()
                        .position(|label| match label {
                            RowMeta { label: name, .. } => name == &key.column,
                            _ => false,
                        })
                        .unwrap_or_else(|| {
                            panic!("Sort column '{}' not found in labels", key.column)
                        });

                    (idx, &key.direction)
                })
                .collect();

            // Sort using stable sort with multiple keys
            rows.sort_by(|a, b| {
                for (idx, direction) in &indices {
                    let left = &a[*idx];
                    let right = &b[*idx];
                    let ordering = left.cmp(right);
                    match direction {
                        SortDirection::Asc => {
                            if ordering != std::cmp::Ordering::Equal {
                                return ordering;
                            }
                        }
                        SortDirection::Desc => {
                            if ordering != std::cmp::Ordering::Equal {
                                return ordering.reverse();
                            }
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });

            (current_labels, Box::new(rows.into_iter()), current_schema, current_store, next)
        }
        QueryPlan::Project { expressions, next, .. } => {
            if input.is_none() {
                // Free-standing projection like `SELECT 1`
                let mut labels = vec![];
                let mut values = vec![];

                for (idx, expr) in expressions.into_iter().enumerate() {
                    let value = evaluate::<NopStore>(expr.expression, None, None).unwrap();
                    labels.push(RowMeta {
                        value: ValueKind::from(&value),
                        label: format!("{}", idx + 1),
                    });
                    values.push(value);
                }

                return Ok((labels, Box::new(vec![values].into_iter())));
            }

            let input_iter = input.ok_or("missing rows").unwrap();
            let schema_name = current_schema.as_ref().ok_or("missing schema").unwrap();
            let store_name = current_store.as_ref().ok_or("missing store").unwrap();

            let store = rx.schema(schema_name).unwrap().get(store_name.deref()).unwrap();

            let labels: Vec<RowMeta> = expressions
                .iter()
                .enumerate()
                .map(|(idx, expr)| match &expr.expression {
                    Expression::Column(name) => store
                        .get_column(name)
                        .ok()
                        .map(|c| RowMeta { value: c.value, label: c.name })
                        .unwrap_or(RowMeta {
                            value: ValueKind::Undefined,
                            label: name.to_string(),
                        }),
                    _ => RowMeta { value: ValueKind::Undefined, label: format!("{}", idx + 1) },
                })
                .collect();

            let store_ref = store;

            let projected_rows = input_iter.map(move |row| {
                expressions
                    .iter()
                    .map(|expr| {
                        evaluate(expr.expression.clone(), Some(&row), Some(store_ref))
                            .unwrap_or(Value::Undefined)
                    })
                    .collect::<Vec<Value>>()
            });

            (labels, Box::new(projected_rows), current_schema, current_store, next)
        }
        QueryPlan::Aggregate { group_by, project, next } => {
            let input_iter = input.ok_or("Missing input for Aggregate").unwrap();

            let schema_name = current_schema.as_ref().ok_or("Missing schema").unwrap();
            let store_name = current_store.as_ref().ok_or("Missing store").unwrap();

            let store = rx.schema(schema_name).unwrap().get(store_name.deref()).unwrap();

            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub struct GroupKey(Vec<GroupPart>);

            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub enum GroupPart {
                Int2(i16),
                Bool(bool),
                Text(String),
                Undefined,
            }
            todo!();
            //
            // let group_columns: Vec<usize> = group_by
            //     .iter()
            //     .map(|expr| match expr {
            //         Expression::Column(name) => store.get_column_index(name).unwrap(),
            //         _ => panic!("Only identifier expressions are supported in GROUP BY"),
            //     })
            //     .collect();
            //
            // let mut groups: HashMap<GroupKey, Vec<Vec<Value>>> = HashMap::new();
            //
            // for row in input_iter {
            //     let mut parts = Vec::with_capacity(group_columns.len());
            //
            //     for &index in &group_columns {
            //         let value = row.get(index).unwrap().clone();
            //         let part = match value {
            //             Value::Int2(v) => GroupPart::Int2(v),
            //             Value::Bool(v) => GroupPart::Bool(v),
            //             Value::Text(ref v) => GroupPart::Text(v.clone()),
            //             Value::Undefined => GroupPart::Undefined,
            //             _ => unimplemented!("Unsupported group key type"),
            //         };
            //         parts.push(part);
            //     }
            //
            //     let key = GroupKey(parts);
            //     groups.entry(key).or_default().push(row);
            // }

            // fn avg_values(args: &[Value]) -> Value {
            //     let mut sum = Value::Float8(0.0);
            //     let mut count = 0usize;
            //
            //     for arg in args {
            //         match arg {
            //             Value::Int2(a) => {
            //                 match &mut sum {
            //                     Value::Float8(v) => {
            //                         *v += *a as f64;
            //                     }
            //                     _ => unimplemented!(),
            //                 }
            //                 count += 1;
            //             }
            //             _ => unimplemented!(),
            //         }
            //     }
            //
            //     if count == 0 {
            //         Value::Undefined
            //     } else {
            //         match sum {
            //             Value::Float8(sum) => Value::Float8(sum / count as f64),
            //             _ => unimplemented!(),
            //         }
            //     }
            // }
            //
            // let mut result_rows = Vec::new();
            //
            // for (key, rows) in groups {
            //     let mut row: Vec<Value> = key
            //         .0
            //         .iter()
            //         .map(|part| match part {
            //             GroupPart::Int2(v) => Value::Int2(*v),
            //             GroupPart::Bool(v) => Value::Bool(*v),
            //             GroupPart::Text(v) => Value::Text(v.clone()),
            //             GroupPart::Undefined => Value::Undefined,
            //         })
            //         .collect();
            //
            //     for agg in &project {
            //         match agg {
            //             Expression::Call(call) => {
            //                 let mut exec = Executor { functions: FunctionRegistry::new() };
            //                 exec.functions.register(AvgFunction {});
            //
            //                 let result = exec
            //                     .eval_function_aggregate(
            //                         "avg",
            //                         call.args.clone(),
            //                         rows.clone(),
            //                         Some(store),
            //                     )
            //                     .unwrap();
            //
            //                 row.push(result);
            //             }
            //             Expression::Column(_) => {
            //                 // already included via group key
            //             }
            //             _ => unimplemented!(),
            //         }
            //     }
            //
            //     result_rows.push(row);
            // }
            //
            // // Generate labels: group key labels + aggregate labels
            // let mut labels: Vec<RowMeta> = group_by
            //     .iter()
            //     .map(|expr| match expr {
            //         Expression::Column(name) => {
            //             RowMeta { value: ValueKind::Undefined, label: name.clone() }
            //         }
            //         _ => RowMeta { value: ValueKind::Undefined, label: "group".to_string() },
            //     })
            //     .collect();
            //
            // labels.extend(project.iter().filter_map(|agg| match agg {
            //     Expression::Call(call) => Some(RowMeta {
            //         value: ValueKind::Undefined,
            //         label: format!("{}", call.func.name),
            //     }),
            //     _ => None,
            // }));
            //
            // (labels, Box::new(result_rows.into_iter()), current_schema, current_store, next)
        }
    };

    if let Some(next_node) = next {
        execute_node(*next_node, rx, labels, schema, store, Some(result_iter))
    } else {
        Ok((labels, result_iter))
    }
}

pub fn evaluate<S: StoreRx>(
    expr: Expression,
    row: Option<&Row>,
    store: Option<&S>,
) -> Result<Value, String> {
    match expr {
        Expression::Column(name) => {
            let store = store.ok_or("Store required for identifier evaluation")?;
            let row = row.ok_or("Row required for identifier evaluation")?;
            let index =
                store.get_column_index(&name).map_err(|_| format!("Unknown column '{}'", name))?;
            row.get(index).cloned().ok_or_else(|| format!("No value for column '{}'", name))
        }

        Expression::Constant(value) => Ok(value),

        Expression::Prefix(prefix) => {
            let value = evaluate::<S>(*prefix.expression, row, store).unwrap();
            let result = match value {
                Value::Int2(value) => match prefix.operator {
                    PrefixOperator::Minus => Value::Int2(value * -1),
                    PrefixOperator::Plus => Value::Int2(value),
                },

                _ => unimplemented!(),
            };

            Ok(result)
        }

        Expression::Call(call) => {
            let mut exec = Executor { functions: FunctionRegistry::new() };
            exec.functions.register(math::AbsFunction {});
            exec.functions.register(math::AvgFunction {});

            Ok(exec.eval_function_scalar(&call.func.name, call.args, row, store).unwrap())
        }

        Expression::Add(left, right) => {
            let left = evaluate::<S>(*left, row, store)?;
            let right = evaluate::<S>(*right, row, store)?;
            Ok(left.add(right))
        }

        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}
