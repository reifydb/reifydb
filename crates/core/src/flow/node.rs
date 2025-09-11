use serde::{Deserialize, Serialize};

use crate::{
	JoinType, SortKey,
	flow::FlowNodeSchema,
	interface::{
		FlowEdgeId, FlowNodeId, TableId, ViewId,
		evaluate::expression::Expression,
	},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowNodeType {
	SourceInlineData {},
	SourceTable {
		name: String,
		table: TableId,
		schema: FlowNodeSchema,
	},
	SourceView {
		name: String,
		view: ViewId,
		schema: FlowNodeSchema,
	},
	Operator {
		operator: OperatorType,
		input_schemas: Vec<FlowNodeSchema>,
		output_schema: FlowNodeSchema,
	},
	SinkView {
		name: String,
		view: ViewId,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType {
	Filter {
		conditions: Vec<Expression<'static>>,
	},
	Map {
		expressions: Vec<Expression<'static>>,
	},
	Extend {
		expressions: Vec<Expression<'static>>,
	},
	MapTerminal {
		expressions: Vec<Expression<'static>>,
		view_id: ViewId,
	},
	Join {
		join_type: JoinType,
		left: Vec<Expression<'static>>,
		right: Vec<Expression<'static>>,
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
}

impl OperatorType {
	/// Returns true if this operator maintains internal state that needs to
	/// be persisted across incremental updates
	pub fn is_stateful(&self) -> bool {
		match self {
			// Stateless operator - pure transformations
			OperatorType::Filter {
				..
			} => false,
			OperatorType::Map {
				..
			} => false,
			OperatorType::Extend {
				..
			} => false,
			OperatorType::MapTerminal {
				..
			} => false,
			OperatorType::Union => false,

			OperatorType::Join {
				..
			} => true,
			OperatorType::Aggregate {
				..
			} => true,
			OperatorType::Take {
				..
			} => true,
			OperatorType::Sort {
				..
			} => true,
			OperatorType::Distinct {
				..
			} => true,
			OperatorType::Apply {
				..
			} => true, // Apply operators are always mod
		}
	}
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
	pub fn new(
		id: impl Into<FlowEdgeId>,
		source: impl Into<FlowNodeId>,
		target: impl Into<FlowNodeId>,
	) -> Self {
		Self {
			id: id.into(),
			source: source.into(),
			target: target.into(),
		}
	}
}
