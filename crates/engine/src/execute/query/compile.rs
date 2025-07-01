// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::aggregate::AggregateNode;
use crate::execute::query::join::LeftJoinNode;
use crate::execute::query::order::OrderNode;
use crate::execute::query::project::ProjectWithoutInputNode;
use crate::execute::query::{FilterNode, LimitNode, Node, ProjectNode, ScanFrameNode};
use crate::frame::{Column, ColumnValues, Frame};
use crate::function::Functions;
use reifydb_catalog::Catalog;
use reifydb_catalog::key::TableRowKey;
use reifydb_core::Kind;
use reifydb_core::interface::Rx;
use reifydb_core::row::Layout;
use reifydb_rql::plan::QueryPlan;

pub(crate) fn compile(
    mut plan: QueryPlan,
    rx: &mut impl Rx,
    functions: Functions,
) -> Box<dyn Node> {
    let mut result: Option<Box<dyn Node>> = None;

    loop {
        plan = match plan {
            QueryPlan::Aggregate { by: group_by, project, next } => {
                let input = result.expect("aggregate requires input");
                result =
                    Some(Box::new(AggregateNode::new(input, group_by, project, functions.clone())));
                if let Some(next) = next {
                    *next
                } else {
                    break;
                }
            }
            QueryPlan::LeftJoin { left, right, on, next } => {
                let left_node = compile(*left, rx, functions.clone());
                let right_node = compile(*right, rx, functions.clone());
                result = Some(Box::new(LeftJoinNode::new(left_node, right_node, on)));
                if let Some(next) = next {
                    *next
                } else {
                    break;
                }
            }
            QueryPlan::Limit { limit, next } => {
                let input = result.expect("limit requires input");
                result = Some(Box::new(LimitNode::new(input, limit)));
                if let Some(next) = next {
                    *next
                } else {
                    break;
                }
            }

            QueryPlan::Filter { expression, next } => {
                // FIXME if multiple filter expressions follow each other - dump then into one node

                let input = result.expect("filter requires input");
                result = Some(Box::new(FilterNode::new(input, vec![expression])));

                if let Some(next) = next {
                    *next
                } else {
                    break;
                }
            }

            QueryPlan::Order { order_by, next } => {
                let input = result.expect("order requires input");
                result = Some(Box::new(OrderNode::new(input, order_by)));
                if let Some(next) = next {
                    *next
                } else {
                    break;
                }
            }

            QueryPlan::Project { expressions, next } => {
                if let Some(input) = result {
                    result = Some(Box::new(ProjectNode::new(input, expressions)));
                } else {
                    result = Some(Box::new(ProjectWithoutInputNode::new(expressions)))
                }

                if let Some(next) = next {
                    *next
                } else {
                    break;
                }
            }

            QueryPlan::ScanTable { schema, table, next, .. } => {
                let schema = Catalog::get_schema_by_name(rx, &schema).unwrap().unwrap();
                let table = Catalog::get_table_by_name(rx, schema.id, &table).unwrap().unwrap();

                let columns = table.columns;

                let values = columns.iter().map(|c| c.value).collect::<Vec<_>>();
                let layout = Layout::new(&values);

                let columns: Vec<Column> = columns
                    .iter()
                    .map(|col| {
                        let name = col.name.clone();
                        let data = match col.value {
                            Kind::Bool => ColumnValues::bool(vec![]),
                            Kind::Float4 => ColumnValues::float4(vec![]),
                            Kind::Float8 => ColumnValues::float8(vec![]),
                            Kind::Int1 => ColumnValues::int1(vec![]),
                            Kind::Int2 => ColumnValues::int2(vec![]),
                            Kind::Int4 => ColumnValues::int4(vec![]),
                            Kind::Int8 => ColumnValues::int8(vec![]),
                            Kind::Int16 => ColumnValues::int16(vec![]),
                            Kind::Text => ColumnValues::string(vec![]),
                            Kind::Uint1 => ColumnValues::uint1(vec![]),
                            Kind::Uint2 => ColumnValues::uint2(vec![]),
                            Kind::Uint4 => ColumnValues::uint4(vec![]),
                            Kind::Uint8 => ColumnValues::uint8(vec![]),
                            Kind::Uint16 => ColumnValues::uint16(vec![]),
                            Kind::Undefined => ColumnValues::Undefined(0),
                        };
                        Column { name, data }
                    })
                    .collect();

                let mut frame = Frame::new_with_name(columns, table.name);

                frame
                    .append_rows(
                        &layout,
                        rx.scan_range(TableRowKey::full_scan(table.id))
                            .unwrap()
                            .into_iter()
                            .map(|versioned| versioned.row),
                    )
                    .unwrap();

                result = Some(Box::new(ScanFrameNode::new(frame)));

                // If there is a next node, continue walking
                if let Some(next) = next {
                    *next
                } else {
                    break;
                }
            }

            QueryPlan::Describe { .. } => {
                unimplemented!("Unsupported plan node in bottom-up compilation");
            }
        };
    }

    result.expect("Failed to construct root node")
}
