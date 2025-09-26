use reifydb_core::interface::FlowNodeId;
use serde::{Deserialize, Serialize};

use super::{Schema, SerializedRow, Store};

/// The complete join state
pub(crate) struct JoinState {
	// Schema is stored separately and loaded once
	pub(crate) schema: Schema,
	// Store for left side entries
	pub(crate) left_store: Store<JoinSideEntry>,
	// Store for right side entries
	pub(crate) right_store: Store<JoinSideEntry>,
}

impl JoinState {
	pub(crate) fn new(node_id: FlowNodeId, schema: Schema) -> Self {
		Self {
			schema,
			left_store: Store::new(node_id, JoinSide::Left),
			right_store: Store::new(node_id, JoinSide::Right),
		}
	}
}

/// Represents rows stored for each side of the join
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinSideEntry {
	pub(crate) rows: Vec<SerializedRow>,
}

/// Which side of the join
#[derive(Debug, Clone, Copy)]
pub(crate) enum JoinSide {
	Left,
	Right,
}
