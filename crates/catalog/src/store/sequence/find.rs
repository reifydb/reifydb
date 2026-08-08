// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	system::ids::sequences::ALL,
};

impl CatalogStore {
	pub(crate) fn find_sequence(rx: &mut Transaction<'_>, sequence_id: SequenceId) -> Result<Option<Sequence>> {
		let Some((_, name)) = ALL.iter().find(|(id, _)| *id == sequence_id) else {
			return_internal_error!(
				"Sequence with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sequence_id
			)
		};
		let namespace = NamespaceId::SYSTEM;

		let sequence_key = SystemSequenceKey::encoded(sequence_id);

		let value = match rx.get(&sequence_key)? {
			Some(bytes) => SHAPE.get::<u64>(&bytes.bytes, VALUE),
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
