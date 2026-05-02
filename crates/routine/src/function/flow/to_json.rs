// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use postcard::from_bytes;
use reifydb_core::{
	common::{JoinType, WindowKind},
	internal,
	sort::SortKey,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::{expression::json::JsonExpression, flow::node::FlowNodeType};
use reifydb_type::{error::Error, value::r#type::Type};
use serde::Serialize;
use serde_json::{Value as JsonValue, to_string, to_value};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

/// JSON-serializable version of FlowNodeType that uses JsonExpression
/// for clean expression serialization without Fragment metadata.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonFlowNodeType {
	SourceInlineData {},
	SourceTable {
		table: u64,
	},
	SourceView {
		view: u64,
	},
	SourceFlow {
		flow: u64,
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
		ts: Option<String>,
	},
}

impl From<&FlowNodeType> for JsonFlowNodeType {
	fn from(node_type: &FlowNodeType) -> Self {
		match node_type {
			FlowNodeType::SourceInlineData {} => JsonFlowNodeType::SourceInlineData {},
			FlowNodeType::SourceTable {
				table,
			} => JsonFlowNodeType::SourceTable {
				table: table.0,
			},
			FlowNodeType::SourceView {
				view,
			} => JsonFlowNodeType::SourceView {
				view: view.0,
			},
			FlowNodeType::SourceFlow {
				flow,
			} => JsonFlowNodeType::SourceFlow {
				flow: flow.0,
			},
			FlowNodeType::SourceRingBuffer {
				ringbuffer,
			} => JsonFlowNodeType::SourceRingBuffer {
				ringbuffer: ringbuffer.0,
			},
			FlowNodeType::SourceSeries {
				series,
			} => JsonFlowNodeType::SourceSeries {
				series: series.0,
			},
			FlowNodeType::Filter {
				conditions,
			} => JsonFlowNodeType::Filter {
				conditions: conditions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::Gate {
				conditions,
			} => JsonFlowNodeType::Gate {
				conditions: conditions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::Map {
				expressions,
			} => JsonFlowNodeType::Map {
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::Extend {
				expressions,
			} => JsonFlowNodeType::Extend {
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::Join {
				join_type,
				left,
				right,
				alias,
				ttl: _,
			} => JsonFlowNodeType::Join {
				join_type: *join_type,
				left: left.iter().map(|e| e.into()).collect(),
				right: right.iter().map(|e| e.into()).collect(),
				alias: alias.clone(),
			},
			FlowNodeType::Aggregate {
				by,
				map,
			} => JsonFlowNodeType::Aggregate {
				by: by.iter().map(|e| e.into()).collect(),
				map: map.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::Append => JsonFlowNodeType::Append,
			FlowNodeType::Sort {
				by,
			} => JsonFlowNodeType::Sort {
				by: by.clone(),
			},
			FlowNodeType::Take {
				limit,
			} => JsonFlowNodeType::Take {
				limit: *limit,
			},
			FlowNodeType::Distinct {
				expressions,
				ttl: _,
			} => JsonFlowNodeType::Distinct {
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::Apply {
				operator,
				expressions,
				ttl: _,
			} => JsonFlowNodeType::Apply {
				operator: operator.clone(),
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::SinkTableView {
				view,
				..
			}
			| FlowNodeType::SinkRingBufferView {
				view,
				..
			}
			| FlowNodeType::SinkSeriesView {
				view,
				..
			} => JsonFlowNodeType::SinkView {
				view: view.0,
			},
			FlowNodeType::SinkSubscription {
				subscription,
			} => JsonFlowNodeType::SinkSubscription {
				subscription: subscription.0.to_string(),
			},
			FlowNodeType::Window {
				kind,
				group_by,
				aggregations,
				ts,
			} => JsonFlowNodeType::Window {
				kind: kind.clone(),
				group_by: group_by.iter().map(|e| e.into()).collect(),
				aggregations: aggregations.iter().map(|e| e.into()).collect(),
				ts: ts.clone(),
			},
		}
	}
}

pub struct FlowNodeToJson {
	info: RoutineInfo,
}

impl Default for FlowNodeToJson {
	fn default() -> Self {
		Self::new()
	}
}

impl FlowNodeToJson {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("flow_node::to_json"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for FlowNodeToJson {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
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

						// Deserialize from postcard
						let node_type: FlowNodeType = from_bytes(bytes).map_err(|e| {
							Error(Box::new(internal!(
								"Failed to deserialize FlowNodeType: {}",
								e
							)))
						})?;

						// Convert to JsonFlowNodeType for clean serialization
						let json_node_type: JsonFlowNodeType = (&node_type).into();

						// Serialize to JSON (untagged - extract inner value only)
						let json_value = to_value(&json_node_type).map_err(|e| {
							Error(Box::new(internal!(
								"Failed to serialize FlowNodeType to JSON: {}",
								e
							)))
						})?;

						// Extract the inner object from the tagged enum {"variant_name": {...}}
						let inner_value = match json_value {
							JsonValue::Object(map) if map.len() == 1 => map
								.into_iter()
								.next()
								.map(|(_, v)| v)
								.unwrap_or(JsonValue::Null),
							JsonValue::String(_) => {
								// Unit variants serialize as strings, return null for
								// untagged
								JsonValue::Null
							}
							other => other,
						};

						let json = to_string(&inner_value).map_err(|e| {
							Error(Box::new(internal!(
								"Failed to serialize FlowNodeType to JSON: {}",
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

impl Function for FlowNodeToJson {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
