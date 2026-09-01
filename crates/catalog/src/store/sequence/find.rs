// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::catalog::id::{NamespaceId, SequenceId},
	key::system::SystemSequenceKey,
	return_internal_error,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::sequence::{Sequence, generator::u64::decode},
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

		let value = match rx.single() {
			Some(single) => match single.begin_query([&sequence_key])?.get(&sequence_key)? {
				Some(row) => decode(EncodedPodRow::view(&row.bytes))?,
				None => 0,
			},
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

#[cfg(test)]
mod tests {
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		store::sequence::system::SystemSequence,
		system::ids::sequences::{NAMESPACE, SINK_CONNECTOR},
	};

	#[test]
	fn the_reported_counter_is_the_one_the_generator_actually_issued() {
		// Writer and reader once declared separate layouts, so this reported 0 forever while ids issued fine.
		let mut txn = create_test_admin_transaction();

		let issued = SystemSequence::next_namespace_id(&mut txn).unwrap();

		let found = CatalogStore::find_sequence(&mut Transaction::Admin(&mut txn), NAMESPACE).unwrap().unwrap();
		assert_eq!(found.value, issued.0);
	}

	#[test]
	fn each_issue_advances_the_reported_counter_by_one() {
		// A reader returning a constant passes a single-value check, so only movement proves it reads storage.
		let mut txn = create_test_admin_transaction();

		SystemSequence::next_namespace_id(&mut txn).unwrap();
		let before = CatalogStore::find_sequence(&mut Transaction::Admin(&mut txn), NAMESPACE)
			.unwrap()
			.unwrap()
			.value;
		SystemSequence::next_namespace_id(&mut txn).unwrap();
		let after = CatalogStore::find_sequence(&mut Transaction::Admin(&mut txn), NAMESPACE)
			.unwrap()
			.unwrap()
			.value;

		assert_eq!(after, before + 1);
	}

	#[test]
	fn a_sequence_never_issued_from_reports_zero_rather_than_failing() {
		// Absent must not read as corrupt, otherwise a fresh database cannot list system.sequences at all.
		let mut txn = create_test_admin_transaction();

		let found = CatalogStore::find_sequence(&mut Transaction::Admin(&mut txn), SINK_CONNECTOR)
			.unwrap()
			.unwrap();

		assert_eq!(found.value, 0);
	}
}
