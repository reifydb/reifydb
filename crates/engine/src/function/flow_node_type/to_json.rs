// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::{JoinType, SortKey, WindowSize, WindowSlide, WindowType, value::column::ColumnData};
use reifydb_rql::{expression::json::JsonExpression, flow::FlowNodeType};
use reifydb_type::internal;
use serde::Serialize;

use crate::function::{ScalarFunction, ScalarFunctionContext};

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
	Merge,
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
			FlowNodeType::Merge => JsonFlowNodeType::Merge,
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

pub struct FlowNodeTypeToJson;

impl FlowNodeTypeToJson {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for FlowNodeTypeToJson {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
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
								reifydb_core::Error(internal!(
									"Failed to deserialize FlowNodeType: {}",
									e
								))
							})?;

						// Convert to JsonFlowNodeType for clean serialization
						let json_node_type: JsonFlowNodeType = (&node_type).into();

						// Serialize to JSON (untagged - extract inner value only)
						let json_value =
							serde_json::to_value(&json_node_type).map_err(|e| {
								reifydb_core::Error(internal!(
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
							reifydb_core::Error(internal!(
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
			_ => Err(reifydb_core::Error(internal!("flow_node_type::to_json only supports Blob input"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		JoinType, SortDirection, SortKey, WindowSize, WindowType,
		interface::{ColumnIdentifier, ColumnSource, FlowId, TableId, ViewId},
		value::{
			column::{Column, Columns},
			container::BlobContainer,
		},
	};
	use reifydb_rql::{
		expression::{
			AliasExpression, ColumnExpression, ConstantExpression, Expression, GreaterThanExpression,
			IdentExpression,
		},
		flow::FlowNodeType,
	};
	use reifydb_type::{
		Fragment,
		value::{Blob, constraint::bytes::MaxBytes},
	};

	use super::*;

	// Helper functions to create expressions for tests
	fn column_expr(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			source: ColumnSource::Source {
				namespace: Fragment::Internal {
					text: Arc::new("_context".to_string()),
				},
				source: Fragment::Internal {
					text: Arc::new("_context".to_string()),
				},
			},
			name: Fragment::Internal {
				text: Arc::from(name.to_string()),
			},
		}))
	}

	fn constant_number(val: &str) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::Internal {
				text: Arc::from(val.to_string()),
			},
		})
	}

	fn greater_than_expr(left: Expression, right: Expression) -> Expression {
		Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(left),
			right: Box::new(right),
			fragment: Fragment::Internal {
				text: Arc::new(">".to_string()),
			},
		})
	}

	fn alias_expr(name: &str, expr: Expression) -> Expression {
		Expression::Alias(AliasExpression {
			alias: IdentExpression(Fragment::Internal {
				text: Arc::from(name.to_string()),
			}),
			expression: Box::new(expr),
			fragment: Fragment::Internal {
				text: Arc::new("as".to_string()),
			},
		})
	}

	fn create_blob_from_node_type(node_type: &FlowNodeType) -> Blob {
		let bytes = postcard::to_stdvec(node_type).unwrap();
		Blob::new(bytes)
	}

	fn test_node_type(node_type: FlowNodeType, expected_json: &str) {
		let function = FlowNodeTypeToJson::new();
		let blob = create_blob_from_node_type(&node_type);

		let input_column = Column {
			name: Fragment::internal("data"),
			data: ColumnData::Blob {
				container: BlobContainer::from_vec(vec![blob]),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected Utf8 column data");
		};

		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(&container[0], expected_json);
	}

	#[tokio::test]
	async fn test_source_inline_data() {
		test_node_type(FlowNodeType::SourceInlineData {}, r#"{}"#);
	}

	#[tokio::test]
	async fn test_source_table() {
		test_node_type(
			FlowNodeType::SourceTable {
				table: TableId(123),
			},
			r#"{"table":123}"#,
		);
	}

	#[tokio::test]
	async fn test_source_view() {
		test_node_type(
			FlowNodeType::SourceView {
				view: ViewId(456),
			},
			r#"{"view":456}"#,
		);
	}

	#[tokio::test]
	async fn test_source_flow() {
		test_node_type(
			FlowNodeType::SourceFlow {
				flow: FlowId(789),
			},
			r#"{"flow":789}"#,
		);
	}

	#[tokio::test]
	async fn test_filter() {
		// Filter with condition: age > 18
		test_node_type(
			FlowNodeType::Filter {
				conditions: vec![greater_than_expr(column_expr("age"), constant_number("18"))],
			},
			r#"{"conditions":[{"left":{"name":"age","namespace":"_context","source":"_context","type":"column"},"right":{"type":"number","value":"18"},"type":"greater_than"}]}"#,
		);
	}

	#[tokio::test]
	async fn test_map() {
		// Map with expressions: column "name" aliased as "user_name", column "id"
		test_node_type(
			FlowNodeType::Map {
				expressions: vec![alias_expr("user_name", column_expr("name")), column_expr("id")],
			},
			r#"{"expressions":[{"alias":"user_name","expression":{"name":"name","namespace":"_context","source":"_context","type":"column"},"type":"alias"},{"name":"id","namespace":"_context","source":"_context","type":"column"}]}"#,
		);
	}

	#[tokio::test]
	async fn test_extend() {
		test_node_type(
			FlowNodeType::Extend {
				expressions: vec![],
			},
			r#"{"expressions":[]}"#,
		);
	}

	#[tokio::test]
	async fn test_join() {
		test_node_type(
			FlowNodeType::Join {
				join_type: JoinType::Inner,
				left: vec![],
				right: vec![],
				alias: None,
			},
			r#"{"alias":null,"join_type":"Inner","left":[],"right":[]}"#,
		);
	}

	#[tokio::test]
	async fn test_join_with_alias() {
		test_node_type(
			FlowNodeType::Join {
				join_type: JoinType::Left,
				left: vec![],
				right: vec![],
				alias: Some("t".to_string()),
			},
			r#"{"alias":"t","join_type":"Left","left":[],"right":[]}"#,
		);
	}

	#[tokio::test]
	async fn test_aggregate() {
		test_node_type(
			FlowNodeType::Aggregate {
				by: vec![],
				map: vec![],
			},
			r#"{"by":[],"map":[]}"#,
		);
	}

	#[tokio::test]
	async fn test_merge() {
		test_node_type(FlowNodeType::Merge, r#"null"#);
	}

	#[tokio::test]
	async fn test_sort() {
		test_node_type(
			FlowNodeType::Sort {
				by: vec![SortKey {
					column: Fragment::internal("col"),
					direction: SortDirection::Asc,
				}],
			},
			r#"{"by":[{"column":{"Internal":{"text":"col"}},"direction":"Asc"}]}"#,
		);
	}

	#[tokio::test]
	async fn test_take() {
		test_node_type(
			FlowNodeType::Take {
				limit: 100,
			},
			r#"{"limit":100}"#,
		);
	}

	#[tokio::test]
	async fn test_distinct() {
		test_node_type(
			FlowNodeType::Distinct {
				expressions: vec![],
			},
			r#"{"expressions":[]}"#,
		);
	}

	#[tokio::test]
	async fn test_apply() {
		test_node_type(
			FlowNodeType::Apply {
				operator: "my_operator".to_string(),
				expressions: vec![],
			},
			r#"{"expressions":[],"operator":"my_operator"}"#,
		);
	}

	#[tokio::test]
	async fn test_sink_view() {
		test_node_type(
			FlowNodeType::SinkView {
				view: ViewId(999),
			},
			r#"{"view":999}"#,
		);
	}

	#[tokio::test]
	async fn test_window() {
		test_node_type(
			FlowNodeType::Window {
				window_type: WindowType::Count,
				size: WindowSize::Count(10),
				slide: None,
				group_by: vec![],
				aggregations: vec![],
				min_events: 1,
				max_window_count: None,
				max_window_age: None,
			},
			r#"{"aggregations":[],"group_by":[],"max_window_age":null,"max_window_count":null,"min_events":1,"size":{"Count":10},"slide":null,"window_type":"Count"}"#,
		);
	}

	#[tokio::test]
	async fn test_multiple_rows() {
		let function = FlowNodeTypeToJson::new();

		let node_type1 = FlowNodeType::SourceTable {
			table: TableId(1),
		};
		let node_type2 = FlowNodeType::Take {
			limit: 10,
		};
		let node_type3 = FlowNodeType::Merge;

		let blobs = vec![
			create_blob_from_node_type(&node_type1),
			create_blob_from_node_type(&node_type2),
			create_blob_from_node_type(&node_type3),
		];

		let input_column = Column {
			name: Fragment::internal("data"),
			data: ColumnData::Blob {
				container: BlobContainer::from_vec(blobs),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected Utf8 column data");
		};

		assert_eq!(container.len(), 3);
		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));

		assert_eq!(&container[0], r#"{"table":1}"#);
		assert_eq!(&container[1], r#"{"limit":10}"#);
		assert_eq!(&container[2], r#"null"#);
	}

	#[tokio::test]
	async fn test_invalid_blob_should_error() {
		let function = FlowNodeTypeToJson::new();

		// Create an invalid blob that can't be deserialized as FlowNodeType
		let invalid_blob = Blob::new(vec![0xFF, 0xFF, 0xFF]);

		let input_column = Column {
			name: Fragment::internal("data"),
			data: ColumnData::Blob {
				container: BlobContainer::from_vec(vec![invalid_blob]),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx);
		assert!(result.is_err(), "Expected error for invalid blob input");
	}

	#[tokio::test]
	async fn test_with_null_data() {
		let function = FlowNodeTypeToJson::new();

		let node_type = FlowNodeType::SourceTable {
			table: TableId(1),
		};
		let blob = create_blob_from_node_type(&node_type);

		let bitvec = vec![true, false, true];
		let blobs = vec![blob.clone(), Blob::empty(), blob];

		let input_column = Column {
			name: Fragment::internal("data"),
			data: ColumnData::Blob {
				container: BlobContainer::new(blobs, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};

		let columns = Columns::new(vec![input_column]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Utf8 {
			container,
			..
		} = result
		else {
			panic!("Expected Utf8 column data");
		};

		assert_eq!(container.len(), 3);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1)); // NULL preserved
		assert!(container.is_defined(2));
	}
}
