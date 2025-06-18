// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::frame::{Column, ColumnValues, Frame};
use reifydb_catalog::Catalog;
use reifydb_catalog::key::TableRowKey;
use reifydb_core::row::Layout;
use reifydb_core::{BitVec, ValueKind};
use reifydb_rql::expression::{AliasExpression, Expression};
use reifydb_rql::plan::QueryPlan;
use reifydb_transaction::Rx;

#[derive(Debug, PartialEq)]
pub enum Source {
    None,
    Table { schema: String, table: String },
}

#[derive(Debug)]
pub struct LazyFrame {
    source: Source,
    frame: Frame,
    expressions: Vec<AliasExpression>,
    filter: Vec<Expression>,
    limit: Option<usize>,
}

impl LazyFrame {
    pub fn evaluate(mut self, rx: &mut impl Rx) -> crate::frame::Result<Frame> {
        // FIXME refactor this - comes from calling SELECT directly
        if self.source == Source::None {
            let mut columns = vec![];

            for (idx, expr) in self.expressions.clone().into_iter().enumerate() {
                let expr = expr.expression;

                let value = evaluate(
                    &expr,
                    &Context {
                        column: None,
                        mask: &BitVec::empty(),
                        columns: &[],
                        row_count: 1,
                        limit: None,
                    },
                )
                .unwrap();
                columns.push(Column { name: format!("{}", idx + 1), data: value });
            }

            self.frame = Frame::new(columns);
            return Ok(self.frame);
        }

        self.populate_frame(rx)?;

        let mask = self.compute_mask();

        if self.expressions.is_empty() {
            // FIXME this might need to be filtered
            if !self.filter.is_empty() {
                unimplemented!();
            }
            return Ok(self.frame);
        }

        let columns = self
            .expressions
            .iter()
            .map(|alias_expr| {
                let expr = &alias_expr.expression;
                let alias = alias_expr.alias.clone().unwrap_or(expr.span().fragment);

                let values = evaluate(
                    expr,
                    &Context {
                        column: None,
                        mask: &mask,
                        columns: self.frame.columns.as_slice(),
                        row_count: self.frame.row_count(),
                        limit: self.limit,
                    },
                )
                .unwrap();

                Column { name: alias.clone(), data: values }
            })
            .collect();

        Ok(Frame::new(columns))
    }

    fn compute_mask(&self) -> BitVec {
        let row_count = self.row_count();
        let mut mask = BitVec::new(row_count, true);
        let row_count = self.limit.unwrap_or(self.row_count());

        for filter_expr in &self.filter {
            let result = evaluate(
                filter_expr,
                &Context {
                    column: None,
                    mask: &mask,
                    columns: self.frame.columns.as_slice(),
                    row_count,
                    limit: self.limit,
                },
            )
            .unwrap();
            match result {
                ColumnValues::Bool(values, valid) => {
                    for i in 0..row_count {
                        mask.set(i, mask.get(i) && valid[i] && values[i]);
                    }
                }
                _ => panic!("filter expression must evaluate to a boolean column"),
            }
        }
        mask
    }

    fn populate_frame(&mut self, rx: &mut impl Rx) -> crate::frame::Result<()> {
        let table = match &self.source {
            Source::Table { schema, table } => {
                let schema = Catalog::get_schema_by_name(rx, &schema).unwrap().unwrap(); // FIXME
                Catalog::get_table_by_name(rx, schema.id, &table).unwrap().unwrap() // FIXME
            }
            Source::None => unreachable!(),
        };

        let columns = table.columns;

        let values = columns.iter().map(|c| c.value).collect::<Vec<_>>();
        let layout = Layout::new(&values);

        let columns: Vec<Column> = columns
            .iter()
            .map(|col| {
                let name = col.name.clone();
                let data = match col.value {
                    ValueKind::Bool => ColumnValues::bool(vec![]),
                    ValueKind::Float4 => ColumnValues::float4(vec![]),
                    ValueKind::Float8 => ColumnValues::float8(vec![]),
                    ValueKind::Int1 => ColumnValues::int1(vec![]),
                    ValueKind::Int2 => ColumnValues::int2(vec![]),
                    ValueKind::Int4 => ColumnValues::int4(vec![]),
                    ValueKind::Int8 => ColumnValues::int8(vec![]),
                    ValueKind::Int16 => ColumnValues::int16(vec![]),
                    ValueKind::String => ColumnValues::string(vec![]),
                    ValueKind::Uint1 => ColumnValues::uint1(vec![]),
                    ValueKind::Uint2 => ColumnValues::uint2(vec![]),
                    ValueKind::Uint4 => ColumnValues::uint4(vec![]),
                    ValueKind::Uint8 => ColumnValues::uint8(vec![]),
                    ValueKind::Uint16 => ColumnValues::uint16(vec![]),
                    ValueKind::Undefined => ColumnValues::Undefined(0),
                };
                Column { name, data }
            })
            .collect();

        self.frame = Frame::new(columns);

        self.frame
            .append_rows(
                &layout,
                rx.scan_range(TableRowKey::full_scan(table.id))
                    .unwrap()
                    .into_iter()
                    .map(|versioned| versioned.row),
            )
            .unwrap();

        Ok(())
    }

    fn row_count(&self) -> usize {
        // self.columns.first().map(|col| col.data.len()).unwrap_or(0)
        self.frame.row_count()
    }
}

impl LazyFrame {
    pub fn from_query_plan(plan: QueryPlan) -> Self {
        Self::from_query_plan_internal(plan)
    }

    fn from_query_plan_internal(plan: QueryPlan) -> LazyFrame {
        match plan {
            QueryPlan::ScanTable { schema, table, next } => {
                let frame = LazyFrame {
                    source: Source::Table { schema, table },
                    frame: Frame::empty(),
                    expressions: vec![],
                    filter: vec![],
                    // row_count,
                    limit: None,
                    // sort: None,
                };
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Project { expressions, next } => {
                let frame = LazyFrame {
                    source: Source::None,
                    frame: Frame::empty(),
                    expressions,
                    filter: vec![],
                    // row_count,
                    limit: None,
                    // sort: None,
                };
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }
            _ => panic!("QueryPlan must start with Scan"),
        }
    }

    fn apply(plan: Box<QueryPlan>, mut frame: LazyFrame) -> LazyFrame {
        match *plan {
            QueryPlan::Filter { expression, next } => {
                frame.filter.push(expression);
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Project { expressions, next } => {
                frame.expressions = expressions;
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Limit { limit, next } => {
                frame.limit = Some(limit);
                if let Some(next) = next { Self::apply(next, frame) } else { frame }
            }

            QueryPlan::Sort { keys, next } => {
                // frame.sort = Some(keys);
                // if let Some(next) = next {
                //     Self::apply(next, frame)
                // } else {
                //     frame
                // }
                unimplemented!()
            }

            QueryPlan::Aggregate { group_by, project, next } => {
                // frame.expressions = project;
                // if let Some(next) = next { Self::apply(next, frame) } else { frame }
                unimplemented!()
            }

            QueryPlan::ScanTable { .. } => {
                panic!("Scan must be the root node; got Scan mid-plan")
            }
        }
    }
}
