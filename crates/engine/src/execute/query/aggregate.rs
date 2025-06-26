// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::{Batch, Node};
use crate::frame::aggregate::Aggregate;
use crate::frame::{Column, ColumnValues, Frame, FrameLayout};
use reifydb_rql::expression::{AliasExpression, Expression};
use std::ops::Deref;

pub(crate) struct AggregateNode {
    input: Box<dyn Node>,
    group_by: Vec<AliasExpression>,
    project: Vec<AliasExpression>,
    layout: Option<FrameLayout>,
}

impl AggregateNode {
    pub fn new(
        input: Box<dyn Node>,
        group_by: Vec<AliasExpression>,
        project: Vec<AliasExpression>,
    ) -> Self {
        Self { input, group_by, project, layout: None }
    }
}

impl Node for AggregateNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        while let Some(Batch { mut frame, mask }) = self.input.next()? {
            // TODO: Load and merge multiple batches if needed

            let (keys, aggregates) = parse_keys_and_aggregates(&self.group_by, &self.project)?;

            let groups = frame.group_by_view(&keys)?;

            let mut key_columns: Vec<Column> = keys
                .iter()
                .map(|k| Column { name: k.to_string(), data: ColumnValues::float8([]) })
                .collect();

            let mut aggregate_columns: Vec<Column> = aggregates
                .iter()
                .map(|agg| Column { name: agg.display_name(), data: ColumnValues::undefined(0) })
                .collect();

            for (group_key, indices) in groups {
                for (value, column) in group_key.into_iter().zip(key_columns.iter_mut()) {
                    column.data.push_value(value);
                }

                for (agg, column) in aggregates.iter().zip(aggregate_columns.iter_mut()) {
                    column.data.extend(agg.evaluate(&frame, &indices)?)?;
                }
            }

            let frame = Frame::new(key_columns.into_iter().chain(aggregate_columns).collect());
            self.layout = Some(FrameLayout::from_frame(&frame));
            return Ok(Some(Batch { frame, mask }));
        }

        Ok(None)
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone().or(self.input.layout())
    }
}

fn parse_keys_and_aggregates<'a>(
    group_by: &'a [AliasExpression],
    project: &'a [AliasExpression],
) -> crate::Result<(Vec<&'a str>, Vec<Aggregate>)> {
    let mut keys = Vec::new();
    let mut aggregates = Vec::new();

    for gb in group_by {
        match gb.expression.deref() {
            Expression::Column(c) => keys.push(c.0.fragment.as_str()),
            // _ => return Err(crate::Error::Unsupported("Non-column group by not supported".into())),
            _ => panic!("Non-column group by not supported"),
        }
    }

    for p in project {
        dbg!(&p.expression);
        match p.expression.deref() {
            Expression::Call(call) => {
                let func = call.func.0.fragment.as_str();
                match call.args.first().map(|arg| arg) {
                    Some(Expression::Column(c)) => {
                        let col = c.0.fragment.to_string();
                        let agg = match func {
                            "avg" => Aggregate::Avg(col),
                            "sum" => Aggregate::Sum(col),
                            "count" => Aggregate::Count(col),
                            // _ => return Err(crate::Error::Unsupported(format!("Aggregate function `{}` is not implemented", func))),
                            _ => unimplemented!(),
                        };
                        aggregates.push(agg);
                    }
                    // _ => return Err(crate::Error::Unsupported("Aggregate args must be columns".into())),
                    _ => panic!(),
                }
            }
            // _ => return Err(crate::Error::Unsupported("Expected aggregate call expression".into())),
            _ => panic!(),
        }
    }

    Ok((keys, aggregates))
}
