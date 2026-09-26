// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::{KEYSPACES, KeyspaceVisitor, dispatch},
		state::{KeyspaceId, OperatorStateKey},
		traits::Keyspace,
	},
};
use rusqlite::{Transaction, TransactionBehavior, params};
use tracing::instrument;

use crate::{
	persistent::sqlite::{
		SqlitePersistent,
		registry::TableRegistry,
		route,
		sql::{CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL},
		typed,
	},
	resident::bucket::{Bucket, BucketMap, write::StandardBucket},
	types::{DropMarker, FlushBatch, StagedWrite},
};

impl SqlitePersistent {
	#[instrument(name = "store::operator::persistent::sqlite::flush_batch", level = "debug", skip(self, batch), fields(
		drop_count = batch.drops.len(),
		state_count = batch.writes.len(),
		checkpoint_count = batch.checkpoints.len()
	))]
	pub fn flush_batch(&self, batch: &FlushBatch) {
		if !batch.writes.is_empty() {
			self.mark_state_written();
		}
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().expect("operator state flush ran without an open connection");
		let transaction = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.expect("operator state flush could not begin");
		for marker in &batch.drops {
			match marker {
				DropMarker::OperatorState(operator) => {
					route::drop_operator(&transaction, *operator, self.inner.tables.mask(*operator))
				}
			}
		}

		let created = write_state(&state_of(batch), &transaction, &self.inner.tables);

		for (flow, entry) in &batch.checkpoints {
			match entry {
				Some(version) => transaction
					.prepare_cached(CHECKPOINT_SET_SQL)
					.expect("flow checkpoint write could not be prepared")
					.execute(params![flow.0 as i64, version.0 as i64])
					.expect("flow checkpoint write failed"),
				None => transaction
					.prepare_cached(CHECKPOINT_REMOVE_SQL)
					.expect("flow checkpoint delete could not be prepared")
					.execute(params![flow.0 as i64])
					.expect("flow checkpoint delete failed"),
			};
		}

		transaction.commit().expect("operator state flush could not commit");
		self.inner.tables.add(&created);
	}
}

fn state_of(batch: &FlushBatch) -> BucketMap {
	let mut state = BucketMap::default();
	for (operator, key, write) in &batch.writes {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
			.expect("a staged operator state key must decode as one");
		let post = match write {
			StagedWrite::Set(row) => Some(row.clone()),
			StagedWrite::Remove => None,
		};
		state.record_bytes(*operator, keyspace, group, suffix, post);
	}
	state
}

fn write_state(state: &BucketMap, txn: &Transaction, tables: &TableRegistry) -> Vec<(OperatorId, KeyspaceId)> {
	let mut created = Vec::new();
	for ((operator, keyspace), bucket) in state.buckets() {
		let made = dispatch(
			*keyspace,
			Write {
				bucket,
				txn,
				tables,
			},
		)
		.expect("a bucketed keyspace must be in the catalogue");
		if made {
			created.push((*operator, *keyspace));
		}
	}
	created
}

struct Write<'a> {
	bucket: &'a dyn Bucket,
	txn: &'a Transaction<'a>,
	tables: &'a TableRegistry,
}

impl KeyspaceVisitor for Write<'_> {
	type Output = bool;

	fn visit<K: Keyspace>(self) -> Self::Output {
		let bucket = self
			.bucket
			.as_any()
			.downcast_ref::<StandardBucket<K>>()
			.expect("a keyspace id must map to exactly one key type");
		let operator = bucket.operator();
		let mut sets: Vec<(K::GroupedKey, Vec<u8>)> = Vec::new();
		let mut removes: Vec<K::GroupedKey> = Vec::new();
		for (group, suffix, entry) in bucket.entries() {
			let key = K::join(group, suffix.clone());
			match &entry.post {
				Some(row) => sets.push((key, row.as_slice().to_vec())),
				None => removes.push(key),
			}
		}
		let exists = self.tables.mask(operator).holds(K::ID);
		if !exists && sets.is_empty() {
			return false;
		}
		if !exists {
			let spec = KEYSPACES
				.iter()
				.find(|spec| spec.id == K::ID)
				.expect("a bucketed keyspace must be in the catalogue");
			self.txn.execute_batch(&typed::create_table(spec, operator))
				.expect("operator state table could not be created");
		}
		typed::set_chunked::<K>(self.txn, operator, &sets);
		if exists {
			typed::remove_chunked::<K>(self.txn, operator, &removes);
		}
		!exists
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::{keyspace::join::JoinLeft, state::GroupId, traits::Keyspace},
			typed::direction::Asc,
		},
	};
	use reifydb_value::{
		util::{cowvec::CowVec, hash::Hash128},
		value::row_number::RowNumber,
	};

	use crate::{
		persistent::sqlite::fixture::{encode, get, open, remove_one, scan, set_one},
		types::{DropMarker, FlushBatch, StagedWrite},
	};

	const OP: OperatorId = OperatorId(1);

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn suffix(n: u64) -> Asc<RowNumber> {
		Asc(RowNumber(n))
	}

	#[test]
	fn a_flush_writes_every_group_into_the_keyspaces_own_table() {
		let store = open();

		let mut batch = FlushBatch::default();
		let seven = encode::<JoinLeft>(&JoinLeft::join(GroupId::hashed(Hash128(7)), suffix(1)));
		let nine = encode::<JoinLeft>(&JoinLeft::join(GroupId::hashed(Hash128(9)), suffix(2)));
		batch.writes.push((OP, seven, StagedWrite::Set(row("seven"))));
		batch.writes.push((OP, nine, StagedWrite::Set(row("nine"))));
		store.flush_batch(&batch);

		let rows = scan::<JoinLeft>(&store, OP);
		assert_eq!(rows.len(), 2, "every group in the bucket must reach the table, not just the first");
	}

	#[test]
	fn a_flushed_tombstone_deletes_the_row_rather_than_storing_a_none() {
		let store = open();
		let key = JoinLeft::join(GroupId::hashed(Hash128(7)), suffix(1));

		set_one::<JoinLeft>(&store, OP, &key, b"live");
		remove_one::<JoinLeft>(&store, OP, &key);

		assert!(
			scan::<JoinLeft>(&store, OP).is_empty(),
			"a removal must delete the durable row; leaving it behind resurrects state the operator deleted"
		);
	}

	#[test]
	fn a_flushed_row_survives_the_round_trip_through_its_payload() {
		let store = open();
		let key = JoinLeft::join(GroupId::hashed(Hash128(7)), suffix(1));

		set_one::<JoinLeft>(&store, OP, &key, b"payload");

		let stored = get::<JoinLeft>(&store, OP, &key).expect("the row");
		let restored = EncodedPodRow::from(EncodedBytes(CowVec::new(stored)));
		assert_eq!(
			String::from_utf8(restored.body().to_vec()).expect("utf8"),
			"payload",
			"the payload round trip carries the pod header, so reading back the body alone would truncate the row"
		);
	}

	#[test]
	fn a_drop_empties_the_operators_tables_and_a_later_write_reuses_them() {
		// the table outlives the drop, so a later write must land in it without a second CREATE
		let store = open();
		let neighbour = OperatorId(2);
		let key = JoinLeft::join(GroupId::hashed(Hash128(7)), suffix(1));
		set_one::<JoinLeft>(&store, OP, &key, b"before");
		set_one::<JoinLeft>(&store, neighbour, &key, b"neighbour");
		let mut batch = FlushBatch::default();
		batch.drops.push(DropMarker::OperatorState(OP));
		store.flush_batch(&batch);
		assert!(scan::<JoinLeft>(&store, OP).is_empty());
		assert!(store.occupied_keyspaces(OP).is_empty());
		assert_eq!(get::<JoinLeft>(&store, neighbour, &key).as_deref(), Some(b"neighbour".as_slice()));
		set_one::<JoinLeft>(&store, OP, &key, b"after");
		assert_eq!(get::<JoinLeft>(&store, OP, &key).as_deref(), Some(b"after".as_slice()));
	}
}
