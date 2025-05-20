// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod call;
mod display;

use crate::function::math;
use base::expression::{Expression, PrefixOperator};
use base::function::{FunctionMode, FunctionRegistry, FunctionResult};
use base::{Label, Row, Value, ValueKind};
use rql::plan::{Plan, QueryPlan};
use std::ops::Deref;
use std::vec;
use transaction::{CatalogTx, NopStore, Rx, SchemaRx, SchemaTx, StoreRx, StoreToCreate, Tx};

#[derive(Debug)]
pub enum ExecutionResult {
    CreateSchema { schema: String },
    CreateTable { schema: String, table: String },
    InsertIntoTable { schema: String, table: String, inserted: usize },
    Query { labels: Vec<Label>, rows: Vec<Row> },
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
    let (labels, iter) = execute_node(plan, rx, vec![], None, None, None)?;
    Ok(ExecutionResult::Query { labels, rows: iter.collect() })
}

fn execute_node<'a>(
    node: QueryPlan,
    rx: &'a impl Rx,
    current_labels: Vec<Label>,
    current_schema: Option<String>,
    current_store: Option<String>,
    input: Option<Box<dyn Iterator<Item = Vec<Value>> + 'a>>,
) -> crate::Result<(Vec<Label>, Box<dyn Iterator<Item = Vec<Value>> + 'a>)> {
    let (labels, result_iter, schema, store, next): (
        Vec<Label>,
        Box<dyn Iterator<Item = Vec<Value>>>,
        Option<String>,
        Option<String>,
        Option<Box<QueryPlan>>,
    ) = match node {
        QueryPlan::Scan { schema, store, next, .. } => (
            current_labels,
            Box::new(rx.scan(&store, None).unwrap()),
            Some(schema),
            Some(store.to_string()),
            next,
        ),

        QueryPlan::Limit { limit: count, next, .. } => {
            let input_iter = input.ok_or("Missing input for Limit").unwrap();
            (current_labels, Box::new(input_iter.take(count)), current_schema, current_store, next)
        }

        QueryPlan::Project { expressions, next, .. } => {
            if input.is_none() {
                // Free-standing projection like `SELECT 1`
                let mut labels = vec![];
                let mut values = vec![];

                for (idx, expr) in expressions.into_iter().enumerate() {
                    let value = evaluate::<NopStore>(expr, None, None).unwrap();
                    labels.push(Label::Custom {
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

            let labels: Vec<Label> = expressions
                .iter()
                .enumerate()
                .map(|(idx, expr)| match expr {
                    Expression::Identifier(name) => store
                        .get_column(name)
                        .ok()
                        .map(|c| Label::Column { value: c.value, column: c.name })
                        .unwrap_or(Label::Custom {
                            value: ValueKind::Undefined,
                            label: name.to_string(),
                        }),
                    _ => {
                        Label::Custom { value: ValueKind::Undefined, label: format!("{}", idx + 1) }
                    }
                })
                .collect();

            let store_ref = store;

            let projected_rows = input_iter.map(move |row| {
                expressions
                    .iter()
                    .map(|expr| {
                        evaluate(expr.clone(), Some(&row), Some(store_ref))
                            .unwrap_or(Value::Undefined)
                    })
                    .collect::<Vec<Value>>()
            });

            (labels, Box::new(projected_rows), current_schema, current_store, next)
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
        Expression::Identifier(name) => {
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

            match exec
                .eval_function(&call.func.name, FunctionMode::Scalar, call.args, row, store)
                .unwrap()
            {
                FunctionResult::Scalar(value) => Ok(value),
                FunctionResult::Rows(_) => unimplemented!(),
            }
        }

        Expression::Add(left, right) => {
            let left = evaluate::<S>(*left, row, store)?;
            let right = evaluate::<S>(*right, row, store)?;
            Ok(left.add(right))
        }

        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}
