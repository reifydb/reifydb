// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		EncodableKey, QueryTransaction, SchemaId, SequenceId,
		SystemSequenceKey, UnversionedQueryTransaction,
	},
	return_internal_error,
};

use crate::{
	CatalogStore,
	sequence::{
		Sequence,
		layout::sequence::{LAYOUT, VALUE},
		system::*,
	},
};

impl CatalogStore {
	pub fn find_sequence(
		rx: &mut impl QueryTransaction,
		sequence_id: SequenceId,
	) -> crate::Result<Option<Sequence>> {
		let (schema, name) = match sequence_id {
			SCHEMA_SEQ_ID => (SchemaId(1), "schema"),
			STORE_SEQ_ID => (SchemaId(1), "store"),
			COLUMN_SEQ_ID => (SchemaId(1), "column"),
			COLUMN_POLICY_SEQ_ID => (SchemaId(1), "column_policy"),
			FLOW_SEQ_ID => (SchemaId(1), "flow"),
			FLOW_NODE_SEQ_ID => (SchemaId(1), "flow_node_seq"),
			FLOW_EDGE_SEQ_ID => (SchemaId(1), "flow_edge_seq"),
			_ => return_internal_error!(
				"Sequence with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sequence_id
			),
		};

		// Read current value from unversioned storage
		let sequence_key = SystemSequenceKey {
			sequence: sequence_id,
		}
		.encode();

		let value = rx.with_unversioned_query(|tx| {
			match tx.get(&sequence_key)? {
				Some(unversioned_row) => Ok(LAYOUT
					.get_u64(&unversioned_row.row, VALUE)),
				None => Ok(0),
			}
		})?;

		Ok(Some(Sequence {
			id: sequence_id,
			schema,
			name: name.to_string(),
			value,
		}))
	}
}
