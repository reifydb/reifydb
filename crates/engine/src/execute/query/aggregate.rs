// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::columnar::layout::ColumnsLayout;
use crate::columnar::{Column, ColumnData, ColumnQualified};
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use crate::function::{AggregateFunction, AggregateFunctionContext, Functions};
use reifydb_core::OwnedSpan;
use reifydb_core::Value;
use reifydb_core::interface::VersionedReadTransaction;
use reifydb_rql::expression::Expression;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

enum Projection {
    Aggregate { column: String, alias: OwnedSpan, function: Box<dyn AggregateFunction> },
    Group { column: String, alias: OwnedSpan },
}

pub(crate) struct AggregateNode {
    input: Box<dyn ExecutionPlan>,
    by: Vec<Expression>,
    map: Vec<Expression>,
    layout: Option<ColumnsLayout>,
    context: Arc<ExecutionContext>,
}

impl AggregateNode {
    pub fn new(
        input: Box<dyn ExecutionPlan>,
        by: Vec<Expression>,
        map: Vec<Expression>,
        context: Arc<ExecutionContext>,
    ) -> Self {
        Self { input, by, map, layout: None, context }
    }
}

impl ExecutionPlan for AggregateNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn VersionedReadTransaction) -> crate::Result<Option<Batch>> {
        if self.layout.is_some() {
            return Ok(None);
        }

        let (keys, mut projections) =
            parse_keys_and_aggregates(&self.by, &self.map, &self.context.functions)?;

        let mut seen_groups = HashSet::<Vec<Value>>::new();
        let mut group_key_order: Vec<Vec<Value>> = Vec::new();

        while let Some(Batch { columns }) = self.input.next(ctx, rx)? {
            let groups = columns.group_by_view(&keys)?;

            for (group_key, _) in &groups {
                if seen_groups.insert(group_key.clone()) {
                    group_key_order.push(group_key.clone());
                }
            }

            for projection in &mut projections {
                if let Projection::Aggregate { function, column, .. } = projection {
                    let column = columns.column(column).unwrap();
                    function
                        .aggregate(AggregateFunctionContext { column, groups: &groups })
                        .unwrap();
                }
            }
        }

        let mut result_columns = Vec::new();

        for projection in projections {
            match projection {
                Projection::Group { alias, column, .. } => {
                    let col_idx = keys.iter().position(|k| k == &column).unwrap();

                    let mut c = Column::ColumnQualified(ColumnQualified {
                        name: alias.fragment.clone(),
                        data: ColumnData::int2_with_capacity(group_key_order.len()),
                    });
                    for key in &group_key_order {
                        c.data_mut().push_value(key[col_idx].clone());
                    }
                    result_columns.push(c);
                }
                Projection::Aggregate { alias, mut function, .. } => {
                    let (keys_out, mut data) = function.finalize().unwrap();
                    align_column_data(&group_key_order, &keys_out, &mut data).unwrap();
                    result_columns.push(Column::ColumnQualified(ColumnQualified {
                        name: alias.fragment.clone(),
                        data,
                    }));
                }
            }
        }

        let columns = Columns::new(result_columns);
        self.layout = Some(ColumnsLayout::from_columns(&columns));

        Ok(Some(Batch { columns }))
    }

    fn layout(&self) -> Option<ColumnsLayout> {
        self.layout.clone().or(self.input.layout())
    }
}

fn parse_keys_and_aggregates<'a>(
    by: &'a [Expression],
    project: &'a [Expression],
    functions: &'a Functions,
) -> crate::Result<(Vec<&'a str>, Vec<Projection>)> {
    let mut keys = Vec::new();
    let mut projections = Vec::new();

    for gb in by {
        match gb {
            Expression::Column(c) => {
                keys.push(c.0.fragment.as_str());
                projections
                    .push(Projection::Group { column: c.0.fragment.to_string(), alias: c.span() })
            } // _ => return Err(reifydb_core::Error::Unsupported("Non-column group by not supported".into())),
            _ => panic!("Non-column group by not supported"),
        }
    }

    for p in project {
        match p {
            Expression::Call(call) => {
                let func = call.func.0.fragment.as_str();
                match call.args.first().map(|arg| arg) {
                    Some(Expression::Column(c)) => {
                        let function = functions.get_aggregate(func).unwrap();
                        projections.push(Projection::Aggregate {
                            column: c.0.fragment.to_string(),
                            alias: p.span(),
                            function,
                        });
                    }
                    // _ => return Err(reifydb_core::Error::Unsupported("Aggregate args must be columns".into())),
                    _ => panic!(),
                }
            }
            // _ => return Err(reifydb_core::Error::Unsupported("Expected aggregate call expression".into())),
            _ => panic!(),
        }
    }
    Ok((keys, projections))
}

fn align_column_data(
    group_key_order: &[Vec<Value>],
    keys: &[Vec<Value>],
    data: &mut ColumnData,
) -> crate::Result<()> {
    let mut key_to_index = HashMap::new();
    for (i, key) in keys.iter().enumerate() {
        key_to_index.insert(key, i);
    }

    let reorder_indices: Vec<usize> = group_key_order
        .iter()
        .map(|k| {
            key_to_index.get(k).copied().ok_or_else(|| {
                reifydb_core::error!(reifydb_core::error::diagnostic::engine::frame_error(format!(
                    "Group key {:?} missing in aggregate output",
                    k
                )))
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    data.reorder(&reorder_indices);
    Ok(())
}
