use serde::{Deserialize, Serialize};

use crate::{
	JoinType, SortKey,
	interface::{
		FlowEdgeId, FlowNodeId, TableId, ViewId,
		evaluate::expression::Expression,
	},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowNodeType<'a> {
	SourceInlineData {},
	SourceTable {
		name: String,
		table: TableId,
	},
	Operator {
		operator: OperatorType<'a>,
	},
	SinkView {
		name: String,
		view: ViewId,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType<'a> {
	Filter {
		conditions: Vec<Expression<'a>>,
	},
	Map {
		expressions: Vec<Expression<'a>>,
	},
	Extend {
		expressions: Vec<Expression<'a>>,
	},
	MapTerminal {
		expressions: Vec<Expression<'a>>,
		view_id: ViewId,
	},
	Join {
		join_type: JoinType,
		left: Vec<Expression<'a>>,
		right: Vec<Expression<'a>>,
	},
	Aggregate {
		by: Vec<Expression<'a>>,
		map: Vec<Expression<'a>>,
	},
	Union,
	Sort {
		by: Vec<SortKey>,
	},
	Take {
		limit: usize,
	},
	Distinct {
		expressions: Vec<Expression<'a>>,
	},
}

impl<'a> OperatorType<'a> {
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
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNode<'a> {
	pub id: FlowNodeId,
	pub ty: FlowNodeType<'a>,
	pub inputs: Vec<FlowNodeId>,
	pub outputs: Vec<FlowNodeId>,
}

impl<'a> FlowNode<'a> {
	pub fn new(id: impl Into<FlowNodeId>, ty: FlowNodeType<'a>) -> Self {
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
