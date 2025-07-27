use crate::JoinType;
use crate::expression::Expression;
use crate::interface::Table;
use crate::SortKey;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.0)
    }
}

#[derive(Debug, Clone)]
pub enum NodeType {
    Table { name: String, table: Table },
    Operator { operator: OperatorType },
    View { name: String, table: Table },
}

#[derive(Debug, Clone)]
pub enum OperatorType {
    Filter { predicate: Expression },
    Map { expressions: Vec<Expression> },
    Join { 
        join_type: JoinType,
        left: Vec<Expression>,
        right: Vec<Expression>,
    },
    Aggregate { 
        by: Vec<Expression>, 
        map: Vec<Expression>
    },
    Union,
    TopK { 
        k: usize, 
        sort: Vec<SortKey> 
    },
    Distinct { 
        expressions: Option<Vec<Expression>> 
    },
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub node_type: NodeType,
    pub inputs: Vec<NodeId>,
    pub outputs: Vec<NodeId>,
}
