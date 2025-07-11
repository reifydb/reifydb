// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::aggregate::AggregateNode;
use crate::execute::query::join::LeftJoinNode;
use crate::execute::query::sort::SortNode;
use crate::execute::query::map::MapWithoutInputNode;
use crate::execute::query::{ExecutionPlan, FilterNode, TakeNode, MapNode, ScanFrameNode};
use crate::frame::{Column, ColumnValues, Frame};
use crate::function::Functions;
use reifydb_catalog::Catalog;
use reifydb_catalog::key::TableRowKey;
use reifydb_core::DataType;
use reifydb_core::interface::Rx;
use reifydb_core::row::Layout;
use reifydb_rql::plan::physical;
use reifydb_rql::plan::physical::PhysicalPlan;

pub(crate) fn compile(
    plan: PhysicalPlan,
    rx: &mut impl Rx,
    functions: Functions,
) -> Box<dyn ExecutionPlan> {
    match plan {
        PhysicalPlan::Aggregate(physical::AggregateNode { by, map, input }) => {
            let input_node = compile(*input, rx, functions.clone());
            Box::new(AggregateNode::new(input_node, by, map, functions))
        }

        PhysicalPlan::Filter(physical::FilterNode { conditions, input }) => {
            let input_node = compile(*input, rx, functions);
            Box::new(FilterNode::new(input_node, conditions))
        }

        PhysicalPlan::Take(physical::TakeNode { take, input }) => {
            let input_node = compile(*input, rx, functions);
            Box::new(TakeNode::new(input_node, take))
        }

        PhysicalPlan::Sort(physical::SortNode { by, input }) => {
            let input_node = compile(*input, rx, functions);
            Box::new(SortNode::new(input_node, by))
        }

        PhysicalPlan::Map(physical::MapNode { map, input }) => {
            if let Some(input) = input {
                let input_node = compile(*input, rx, functions);
                Box::new(MapNode::new(input_node, map))
            } else {
                Box::new(MapWithoutInputNode::new(map))
            }
        }

        PhysicalPlan::JoinLeft(physical::JoinLeftNode { left, right, on }) => {
            let left_node = compile(*left, rx, functions.clone());
            let right_node = compile(*right, rx, functions);
            Box::new(LeftJoinNode::new(left_node, right_node, on))
        }

        PhysicalPlan::TableScan(physical::TableScanNode { schema, table }) => {
            // If schema is NONE resolve table directly by name
            let schema =
                Catalog::get_schema_by_name(rx, &schema.as_ref().unwrap().fragment.as_str())
                    .unwrap()
                    .unwrap();

            let table = Catalog::get_table_by_name(rx, schema.id, &table.fragment.as_str())
                .unwrap()
                .unwrap();

            let columns = table.columns;
            let values = columns.iter().map(|c| c.value).collect::<Vec<_>>();
            let layout = Layout::new(&values);

            let columns: Vec<Column> = columns
                .iter()
                .map(|col| {
                    let name = col.name.clone();
                    let data = match col.value {
                        DataType::Bool => ColumnValues::bool(vec![]),
                        DataType::Float4 => ColumnValues::float4(vec![]),
                        DataType::Float8 => ColumnValues::float8(vec![]),
                        DataType::Int1 => ColumnValues::int1(vec![]),
                        DataType::Int2 => ColumnValues::int2(vec![]),
                        DataType::Int4 => ColumnValues::int4(vec![]),
                        DataType::Int8 => ColumnValues::int8(vec![]),
                        DataType::Int16 => ColumnValues::int16(vec![]),
                        DataType::Utf8 => ColumnValues::utf8(vec![]),
                        DataType::Uint1 => ColumnValues::uint1(vec![]),
                        DataType::Uint2 => ColumnValues::uint2(vec![]),
                        DataType::Uint4 => ColumnValues::uint4(vec![]),
                        DataType::Uint8 => ColumnValues::uint8(vec![]),
                        DataType::Uint16 => ColumnValues::uint16(vec![]),
                        DataType::Undefined => ColumnValues::Undefined(0),
                    };
                    Column { name, values: data }
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

            Box::new(ScanFrameNode::new(frame))
        }
        PhysicalPlan::CreateDeferredView(_)
        | PhysicalPlan::CreateSchema(_)
        | PhysicalPlan::CreateTable(_)
        | PhysicalPlan::InsertIntoTable(_) => unreachable!(),
    }
}
