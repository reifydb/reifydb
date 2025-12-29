use reifydb_core::interface::FlowNodeId;
use reifydb_type::RowNumber;
use serde::{Deserialize, Serialize};

use super::Store;

pub(crate) struct JoinState {
	pub(crate) left: Store,
	pub(crate) right: Store,
}

impl JoinState {
	pub(crate) fn new(node_id: FlowNodeId) -> Self {
		Self {
			left: Store::new(node_id, JoinSide::Left),
			right: Store::new(node_id, JoinSide::Right),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinSideEntry {
	pub(crate) rows: Vec<RowNumber>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
	Left,
	Right,
}
