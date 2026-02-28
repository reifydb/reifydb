// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::{NamespaceId, SequenceId},
	key::system_sequence::SystemSequenceKey,
	return_internal_error,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::sequence::{
		Sequence,
		schema::sequence::{SCHEMA, VALUE},
	},
	system::ids::sequences::{COLUMN, COLUMN_PROPERTY, FLOW, FLOW_EDGE, FLOW_NODE, NAMESPACE, PRIMARY_KEY, SOURCE},
};

impl CatalogStore {
	pub(crate) fn find_sequence(rx: &mut Transaction<'_>, sequence_id: SequenceId) -> Result<Option<Sequence>> {
		let (namespace, name) = match sequence_id {
			NAMESPACE => (NamespaceId(1), "namespace"),
			SOURCE => (NamespaceId(1), "source"),
			COLUMN => (NamespaceId(1), "column"),
			COLUMN_PROPERTY => (NamespaceId(1), "column_property"),
			FLOW => (NamespaceId(1), "flow"),
			FLOW_NODE => (NamespaceId(1), "flow_node"),
			FLOW_EDGE => (NamespaceId(1), "flow_edge"),
			PRIMARY_KEY => (NamespaceId(1), "primary_key"),
			_ => return_internal_error!(
				"Sequence with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sequence_id
			),
		};

		// Read current value from single storage
		let sequence_key = SystemSequenceKey::encoded(sequence_id);

		let value = match rx.get(&sequence_key)? {
			Some(row) => SCHEMA.get_u64(&row.values, VALUE),
			None => 0,
		};

		Ok(Some(Sequence {
			id: sequence_id,
			namespace,
			name: name.to_string(),
			value,
		}))
	}
}
