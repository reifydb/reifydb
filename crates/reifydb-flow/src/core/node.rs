use reifydb_core::{
	JoinType, SortKey,
	interface::{
		FlowEdgeId, FlowNodeId, TableId, ViewId, expression::Expression,
	},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowNodeType {
	SourceTable {
		name: String,
		table: TableId,
	},
	Operator {
		operator: OperatorType,
	},
	SinkView {
		name: String,
		view: ViewId,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType {
	Filter {
		conditions: Vec<Expression>,
	},
	Map {
		expressions: Vec<Expression>,
	},
	Join {
		join_type: JoinType,
		left: Vec<Expression>,
		right: Vec<Expression>,
	},
	Aggregate {
		by: Vec<Expression>,
		map: Vec<Expression>,
	},
	Union,
	Sort {
		by: Vec<SortKey>,
	},
	Take {
		limit: usize,
	},
	Distinct {
		expressions: Vec<Expression>,
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
