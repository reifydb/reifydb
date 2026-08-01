// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_core::{
	common::{JoinType, WindowKind},
	internal,
	sort::SortKey,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::{expression::json::JsonExpression, flow::operator::OperatorDef};
use reifydb_value::{error::Error, value::value_type::ValueType};
use serde::Serialize;
use serde_json::{Value as JsonValue, to_string, to_value};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonOperatorDef {
	SourceInlineData {},
	SourceTable {
		table: u64,
	},
	SourceView {
		view: u64,
	},
	SourceRingBuffer {
		ringbuffer: u64,
	},
	SourceSeries {
		series: u64,
	},
	Filter {
		conditions: Vec<JsonExpression>,
	},
	Gate {
		conditions: Vec<JsonExpression>,
	},
	Map {
		expressions: Vec<JsonExpression>,
	},
	Extend {
		expressions: Vec<JsonExpression>,
	},
	Join {
		join_type: JoinType,
		left: Vec<JsonExpression>,
		right: Vec<JsonExpression>,
		alias: Option<String>,
	},
	Aggregate {
		by: Vec<JsonExpression>,
		map: Vec<JsonExpression>,
	},
	Append,
	Sort {
		by: Vec<SortKey>,
	},
	Take {
		limit: usize,
	},
	Distinct {
		expressions: Vec<JsonExpression>,
	},
	Apply {
		operator: String,
		expressions: Vec<JsonExpression>,
	},
	SinkView {
		view: u64,
	},
	SinkSubscription {
		subscription: String,
	},
	Window {
		kind: WindowKind,
		group_by: Vec<JsonExpression>,
		aggregations: Vec<JsonExpression>,
	},
}

impl From<&OperatorDef> for JsonOperatorDef {
	fn from(node_type: &OperatorDef) -> Self {
		match node_type {
			OperatorDef::SourceInlineData {} => JsonOperatorDef::SourceInlineData {},
			OperatorDef::SourceTable {
				table,
			} => JsonOperatorDef::SourceTable {
				table: table.0,
			},
			OperatorDef::SourceView {
				view,
			} => JsonOperatorDef::SourceView {
				view: view.0,
			},
			OperatorDef::SourceRingBuffer {
				ringbuffer,
			} => JsonOperatorDef::SourceRingBuffer {
				ringbuffer: ringbuffer.0,
			},
			OperatorDef::SourceSeries {
				series,
			} => JsonOperatorDef::SourceSeries {
				series: series.0,
			},
			OperatorDef::Filter {
				conditions,
			} => JsonOperatorDef::Filter {
				conditions: conditions.iter().map(|e| e.into()).collect(),
			},
			OperatorDef::Gate {
				conditions,
			} => JsonOperatorDef::Gate {
				conditions: conditions.iter().map(|e| e.into()).collect(),
			},
			OperatorDef::Map {
				expressions,
			} => JsonOperatorDef::Map {
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			OperatorDef::Extend {
				expressions,
			} => JsonOperatorDef::Extend {
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			OperatorDef::Join {
				join_type,
				left,
				right,
				alias,
				snapshot: _,
				natural: _,
				latest: _,
			} => JsonOperatorDef::Join {
				join_type: *join_type,
				left: left.iter().map(|e| e.into()).collect(),
				right: right.iter().map(|e| e.into()).collect(),
				alias: alias.clone(),
			},
			OperatorDef::Aggregate {
				by,
				map,
			} => JsonOperatorDef::Aggregate {
				by: by.iter().map(|e| e.into()).collect(),
				map: map.iter().map(|e| e.into()).collect(),
			},
			OperatorDef::Append {
				..
			} => JsonOperatorDef::Append,
			OperatorDef::Sort {
				by,
			} => JsonOperatorDef::Sort {
				by: by.clone(),
			},
			OperatorDef::Take {
				limit,
			} => JsonOperatorDef::Take {
				limit: *limit,
			},
			OperatorDef::Distinct {
				expressions,
			} => JsonOperatorDef::Distinct {
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			OperatorDef::Apply {
				operator,
				expressions,
			} => JsonOperatorDef::Apply {
				operator: operator.clone(),
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			OperatorDef::SinkTableView {
				view,
				..
			}
			| OperatorDef::SinkRingBufferView {
				view,
				..
			}
			| OperatorDef::SinkSeriesView {
				view,
				..
			} => JsonOperatorDef::SinkView {
				view: view.0,
			},
			OperatorDef::SinkSubscription {
				subscription,
			} => JsonOperatorDef::SinkSubscription {
				subscription: subscription.0.to_string(),
			},
			OperatorDef::Window {
				kind,
				group_by,
				aggregations,
				..
			} => JsonOperatorDef::Window {
				kind: kind.clone(),
				group_by: group_by.iter().map(|e| e.into()).collect(),
				aggregations: aggregations.iter().map(|e| e.into()).collect(),
			},
		}
	}
}

pub struct OperatorDefToJson {
	info: RoutineInfo,
}

impl Default for OperatorDefToJson {
	fn default() -> Self {
		Self::new()
	}
}

impl OperatorDefToJson {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("flow_node::to_json"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for OperatorDefToJson {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.is_empty() {
			return Ok(Columns::new(vec![ColumnWithName::new(
				ctx.fragment.clone(),
				ColumnBuffer::utf8(Vec::<String>::new()),
			)]));
		}

		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		match data {
			ColumnBuffer::Blob {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let bytes = match container.get(i) {
							Some(b) => b,
							None => continue,
						};

						let node_type: OperatorDef = from_bytes(bytes).map_err(|e| {
							Error(Box::new(internal!(
								"Failed to deserialize OperatorDef: {}",
								e
							)))
						})?;

						let json_node_type: JsonOperatorDef = (&node_type).into();

						let json_value = to_value(&json_node_type).map_err(|e| {
							Error(Box::new(internal!(
								"Failed to serialize OperatorDef to JSON: {}",
								e
							)))
						})?;

						let inner_value = match json_value {
							JsonValue::Object(map) if map.len() == 1 => map
								.into_iter()
								.next()
								.map(|(_, v)| v)
								.unwrap_or(JsonValue::Null),
							JsonValue::String(_) => JsonValue::Null,
							other => other,
						};

						let json = to_string(&inner_value).map_err(|e| {
							Error(Box::new(internal!(
								"Failed to serialize OperatorDef to JSON: {}",
								e
							)))
						})?;

						result_data.push(json);
					} else {
						result_data.push(String::new());
					}
				}

				let result_col_data = ColumnBuffer::utf8(result_data);
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv.clone(),
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			_ => Err(RoutineError::FunctionExecutionFailed {
				function: ctx.fragment.clone(),
				reason: "flow_node::to_json only supports Blob input".to_string(),
			}),
		}
	}
}

impl Function for OperatorDefToJson {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
