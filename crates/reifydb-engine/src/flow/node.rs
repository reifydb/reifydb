use std::fmt;

use reifydb_core::{JoinType, SortKey, interface::TableDef};
use reifydb_rql::expression::Expression;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Node({})", self.0)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeType {
	Source {
		name: String,
		table: TableDef,
	},
	Operator {
		operator: OperatorType,
	},
	Sink {
		name: String,
		table: TableDef,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType {
	Filter {
		predicate: Expression,
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
	TopK {
		k: usize,
		sort: Vec<SortKey>,
	},
	Distinct {
		expressions: Option<Vec<Expression>>,
	},
}

impl OperatorType {
	/// Returns true if this operator maintains internal state that needs to
	/// be persisted across incremental updates
	pub fn is_stateful(&self) -> bool {
		match self {
			// Stateless operators - pure transformations
			OperatorType::Filter {
				..
			} => false,
			OperatorType::Map {
				..
			} => false,
			OperatorType::Union => false,

			// Stateful operators - need persistent state for
			// incremental updates
			OperatorType::Join {
				..
			} => true, // Hash tables for both sides
			OperatorType::Aggregate {
				..
			} => true, // Running aggregation state
			OperatorType::TopK {
				..
			} => true, // Sorted buffer of top K elements
			OperatorType::Distinct {
				..
			} => true, // Set of seen data
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
	pub id: NodeId,
	pub ty: NodeType,
	pub inputs: Vec<NodeId>,
	pub outputs: Vec<NodeId>,
}
