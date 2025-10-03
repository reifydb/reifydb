use reifydb_core::interface::FlowNodeId;
use serde::{Deserialize, Serialize};

use super::{Schema, SerializedRow, Store};
use crate::operator::join::store::UndefinedTracker;

pub(crate) struct JoinState {
	pub(crate) schema: Schema,
	pub(crate) left: Store,
	pub(crate) right: Store,
	/// Track which left rows have had undefined joins emitted
	pub(crate) undefined_emitted: UndefinedTracker,
}

impl JoinState {
	pub(crate) fn new(node_id: FlowNodeId, schema: Schema) -> Self {
		Self {
			schema,
			left: Store::new(node_id, JoinSide::Left),
			right: Store::new(node_id, JoinSide::Right),
			undefined_emitted: UndefinedTracker::new(node_id),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JoinSideEntry {
	pub(crate) rows: Vec<SerializedRow>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum JoinSide {
	Left,
	Right,
}
