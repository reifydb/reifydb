use reifydb_core::{
	JoinStrategy, JoinType, SortKey, WindowSize, WindowSlide, WindowType,
	interface::{FlowEdgeId, FlowNodeId, TableId, ViewId, expression::Expression},
};
use serde::{Deserialize, Serialize};

use crate::query::QueryString;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowNodeType {
	SourceInlineData {},
	SourceTable {
		table: TableId,
	},
	SourceView {
		view: ViewId,
	},
	Filter {
		conditions: Vec<Expression<'static>>,
	},
	Map {
		expressions: Vec<Expression<'static>>,
	},
	Extend {
		expressions: Vec<Expression<'static>>,
	},
	Join {
		join_type: JoinType,
		left: Vec<Expression<'static>>,
		right: Vec<Expression<'static>>,
		alias: Option<String>,
		strategy: JoinStrategy,
		right_query: QueryString,
	},
	Aggregate {
		by: Vec<Expression<'static>>,
		map: Vec<Expression<'static>>,
	},
	Union,
	Sort {
		by: Vec<SortKey>,
	},
	Take {
		limit: usize,
	},
	Distinct {
		expressions: Vec<Expression<'static>>,
	},
	Apply {
		operator_name: String,
		expressions: Vec<Expression<'static>>,
	},
	SinkView {
		view: ViewId,
	},
	Window {
		window_type: WindowType,
		size: WindowSize,
		slide: Option<WindowSlide>,
		group_by: Vec<Expression<'static>>,
		aggregations: Vec<Expression<'static>>,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNode {
	pub id: FlowNodeId,
	pub ty: FlowNodeType,
	pub inputs: Vec<FlowNodeId>,
	pub outputs: Vec<FlowNodeId>,
}

impl FlowNode {
	pub fn new(id: impl Into<FlowNodeId>, ty: FlowNodeType) -> Self {
		Self {
			id: id.into(),
			ty,
			inputs: Vec::new(),
			outputs: Vec::new(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowEdge {
	pub id: FlowEdgeId,
	pub source: FlowNodeId,
	pub target: FlowNodeId,
}

impl FlowEdge {
	pub fn new(id: impl Into<FlowEdgeId>, source: impl Into<FlowNodeId>, target: impl Into<FlowNodeId>) -> Self {
		Self {
			id: id.into(),
			source: source.into(),
			target: target.into(),
		}
	}
}
