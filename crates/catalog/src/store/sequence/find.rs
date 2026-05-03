// SPDX-License-Identifier: Apache-2.0
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
		shape::sequence::{SHAPE, VALUE},
	},
	system::ids::sequences::{COLUMN, COLUMN_PROPERTY, FLOW, FLOW_EDGE, FLOW_NODE, NAMESPACE, PRIMARY_KEY, SOURCE},
};

impl CatalogStore {
	pub(crate) fn find_sequence(rx: &mut Transaction<'_>, sequence_id: SequenceId) -> Result<Option<Sequence>> {
		let (namespace, name) = match sequence_id {
			NAMESPACE => (NamespaceId::SYSTEM, "namespace"),
			SOURCE => (NamespaceId::SYSTEM, "source"),
			COLUMN => (NamespaceId::SYSTEM, "column"),
			COLUMN_PROPERTY => (NamespaceId::SYSTEM, "column_property"),
			FLOW => (NamespaceId::SYSTEM, "flow"),
			FLOW_NODE => (NamespaceId::SYSTEM, "flow_node"),
			FLOW_EDGE => (NamespaceId::SYSTEM, "flow_edge"),
			PRIMARY_KEY => (NamespaceId::SYSTEM, "primary_key"),
			_ => return_internal_error!(
				"Sequence with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sequence_id
			),
		};

		let sequence_key = SystemSequenceKey::encoded(sequence_id);

		let value = match rx.get(&sequence_key)? {
			Some(row) => SHAPE.get_u64(&row.row, VALUE),
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
