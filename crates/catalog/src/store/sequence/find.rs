// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{NamespaceId, QueryTransaction, SequenceId, SystemSequenceKey},
	return_internal_error,
};

use crate::{
	CatalogStore,
	store::sequence::{
		Sequence,
		layout::sequence::{LAYOUT, VALUE},
	},
};

impl CatalogStore {
	pub async fn find_sequence(
		rx: &mut impl QueryTransaction,
		sequence_id: SequenceId,
	) -> crate::Result<Option<Sequence>> {
		let (namespace, name) = match sequence_id {
			crate::system::ids::sequences::NAMESPACE => (NamespaceId(1), "namespace"),
			crate::system::ids::sequences::SOURCE => (NamespaceId(1), "source"),
			crate::system::ids::sequences::COLUMN => (NamespaceId(1), "column"),
			crate::system::ids::sequences::COLUMN_POLICY => (NamespaceId(1), "column_policy"),
			crate::system::ids::sequences::FLOW => (NamespaceId(1), "flow"),
			crate::system::ids::sequences::FLOW_NODE => (NamespaceId(1), "flow_node"),
			crate::system::ids::sequences::FLOW_EDGE => (NamespaceId(1), "flow_edge"),
			crate::system::ids::sequences::PRIMARY_KEY => (NamespaceId(1), "primary_key"),
			_ => return_internal_error!(
				"Sequence with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sequence_id
			),
		};

		// Read current value from single storage
		let sequence_key = SystemSequenceKey::encoded(sequence_id);

		let value = match rx.get(&sequence_key).await? {
			Some(row) => LAYOUT.get_u64(&row.values, VALUE),
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
