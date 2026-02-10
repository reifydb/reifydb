// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::{
	common::{JoinType, WindowSize, WindowSlide, WindowType},
	internal,
	sort::SortKey,
	value::column::data::ColumnData,
};
use reifydb_rql::{expression::json::JsonExpression, flow::node::FlowNodeType};
use serde::Serialize;

use crate::{ScalarFunction, ScalarFunctionContext};

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
	Filter {
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
		window_type: WindowType,
		size: WindowSize,
		slide: Option<WindowSlide>,
		group_by: Vec<JsonExpression>,
		aggregations: Vec<JsonExpression>,
		min_events: usize,
		max_window_count: Option<usize>,
		#[serde(serialize_with = "serialize_duration_opt")]
		max_window_age: Option<Duration>,
	},
}

fn serialize_duration_opt<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
	S: serde::Serializer,
{
	match duration {
		Some(d) => serializer.serialize_some(&d.as_secs()),
		None => serializer.serialize_none(),
	}
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
			FlowNodeType::Filter {
				conditions,
			} => JsonFlowNodeType::Filter {
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
			} => JsonFlowNodeType::Distinct {
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::Apply {
				operator,
				expressions,
			} => JsonFlowNodeType::Apply {
				operator: operator.clone(),
				expressions: expressions.iter().map(|e| e.into()).collect(),
			},
			FlowNodeType::SinkView {
				view,
			} => JsonFlowNodeType::SinkView {
				view: view.0,
			},
			FlowNodeType::SinkSubscription {
				subscription,
			} => JsonFlowNodeType::SinkSubscription {
				subscription: subscription.0.to_string(),
			},
			FlowNodeType::Window {
				window_type,
				size,
				slide,
				group_by,
				aggregations,
				min_events,
				max_window_count,
				max_window_age,
			} => JsonFlowNodeType::Window {
				window_type: window_type.clone(),
				size: size.clone(),
				slide: slide.clone(),
				group_by: group_by.iter().map(|e| e.into()).collect(),
				aggregations: aggregations.iter().map(|e| e.into()).collect(),
				min_events: *min_events,
				max_window_count: *max_window_count,
				max_window_age: *max_window_age,
			},
		}
	}
}

pub struct FlowNodeToJson;

impl FlowNodeToJson {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for FlowNodeToJson {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::utf8(Vec::<String>::new()));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Blob {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let blob = &container[i];
						let bytes = blob.as_bytes();

						// Deserialize from postcard
						let node_type: FlowNodeType =
							postcard::from_bytes(bytes).map_err(|e| {
								reifydb_type::error::Error(internal!(
									"Failed to deserialize FlowNodeType: {}",
									e
								))
							})?;

						// Convert to JsonFlowNodeType for clean serialization
						let json_node_type: JsonFlowNodeType = (&node_type).into();

						// Serialize to JSON (untagged - extract inner value only)
						let json_value =
							serde_json::to_value(&json_node_type).map_err(|e| {
								reifydb_type::error::Error(internal!(
									"Failed to serialize FlowNodeType to JSON: {}",
									e
								))
							})?;

						// Extract the inner object from the tagged enum {"variant_name": {...}}
						let inner_value = match json_value {
							serde_json::Value::Object(map) if map.len() == 1 => map
								.into_iter()
								.next()
								.map(|(_, v)| v)
								.unwrap_or(serde_json::Value::Null),
							serde_json::Value::String(_) => {
								// Unit variants serialize as strings, return null for
								// untagged
								serde_json::Value::Null
							}
							other => other,
						};

						let json = serde_json::to_string(&inner_value).map_err(|e| {
							reifydb_type::error::Error(internal!(
								"Failed to serialize FlowNodeType to JSON: {}",
								e
							))
						})?;

						result_data.push(json);
					} else {
						result_data.push(String::new());
					}
				}

				Ok(ColumnData::utf8_with_bitvec(result_data, container.bitvec().clone()))
			}
			_ => Err(reifydb_type::error::Error(internal!("flow_node::to_json only supports Blob input"))
				.into()),
		}
	}
}
