use serde::{Deserialize, Serialize};

pub type Version = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
	Inner,
	Left,
}

impl Default for JoinType {
	fn default() -> Self {
		JoinType::Left
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
	Index,
	Unique,
	Primary,
}

impl Default for IndexType {
	fn default() -> Self {
		IndexType::Index
	}
}
