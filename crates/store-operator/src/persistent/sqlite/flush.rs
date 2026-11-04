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
		sql::{CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL, SAVED_LSN_SET_SQL},
		typed,
	},
	resident::bucket::{Bucket, BucketMap, write::StandardBucket},
	types::{DropMarker, FlushBatch, RetiredRows, StagedWrite},
};

impl SqlitePersistent {
	#[instrument(name = "store::operator::persistent::sqlite::flush_batch", level = "debug", skip(self, batch), fields(
		drop_count = batch.drops.len(),
		state_count = batch.writes.len(),
		checkpoint_count = batch.checkpoints.len()
	))]
	pub fn flush_batch(&self, batch: &FlushBatch) -> Vec<RetiredRows> {
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

		let mut retired = Vec::new();
		for (operator, group) in &batch.retired {
			let mut mask = self.inner.tables.mask(*operator).held();
			for (made, keyspace) in &created {
				if made == operator && !mask.contains(keyspace) {
					mask.push(*keyspace);
				}
			}
			route::retire_group(&transaction, *operator, *group, &mask, &mut retired);
		}

		for (flow, entry) in &batch.checkpoints {
			match entry {
				Some(checkpoint) => transaction
					.prepare_cached(CHECKPOINT_SET_SQL)
					.expect("flow checkpoint write could not be prepared")
					.execute(params![
						flow.0 as i64,
						checkpoint.version.0 as i64,
						checkpoint.lsn.as_u64() as i64
					])
					.expect("flow checkpoint write failed"),
				None => transaction
					.prepare_cached(CHECKPOINT_REMOVE_SQL)
					.expect("flow checkpoint delete could not be prepared")
					.execute(params![flow.0 as i64])
					.expect("flow checkpoint delete failed"),
			};
		}

		if let Some(through) = batch.through {
			transaction
				.prepare_cached(SAVED_LSN_SET_SQL)
				.expect("operator saved lsn write could not be prepared")
				.execute(params![through.as_u64().to_le_bytes().to_vec()])
				.unwrap_or_else(|err| {
					panic!("operator saved lsn {} could not be written: {err}", through.as_u64())
				});
		}

		transaction.commit().expect("operator state flush could not commit");
		self.inner.tables.add(&created);
		retired
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
	use std::panic::{AssertUnwindSafe, catch_unwind};

	use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::{
				keyspace::join::{JoinLeft, JoinRight},
				state::GroupId,
				traits::Keyspace,
			},
			typed::direction::Asc,
		},
	};
	use reifydb_value::{
		util::{cowvec::CowVec, hash::Hash128},
		value::row_number::RowNumber,
	};

	use crate::{
		persistent::sqlite::{
			fixture::{encode, get, open, remove_one, scan, set_one, with_conn},
			typed::table_of,
		},
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
		assert!(store.occupied_keyspaces(OP).unwrap().is_empty());
		assert_eq!(get::<JoinLeft>(&store, neighbour, &key).as_deref(), Some(b"neighbour".as_slice()));
		set_one::<JoinLeft>(&store, OP, &key, b"after");
		assert_eq!(get::<JoinLeft>(&store, OP, &key).as_deref(), Some(b"after".as_slice()));
	}

	fn group(n: u128) -> GroupId {
		GroupId::hashed(Hash128(n))
	}

	#[test]
	fn a_retire_deletes_the_group_from_every_keyspace_and_nothing_else() {
		// a retire that misses a keyspace leaks rows nothing can reach again; one that overreaches loses live
		// state
		let store = open();
		let neighbour = OperatorId(2);
		let left = JoinLeft::join(group(7), suffix(1));
		let right = JoinRight::join(group(7), suffix(1));
		let other_group = JoinLeft::join(group(9), suffix(1));
		set_one::<JoinLeft>(&store, OP, &left, b"gone");
		set_one::<JoinRight>(&store, OP, &right, b"gone");
		set_one::<JoinLeft>(&store, OP, &other_group, b"keep");
		set_one::<JoinLeft>(&store, neighbour, &left, b"keep");

		let mut batch = FlushBatch::default();
		batch.retired.push((OP, group(7)));
		store.flush_batch(&batch);

		assert!(get::<JoinLeft>(&store, OP, &left).is_none());
		assert!(get::<JoinRight>(&store, OP, &right).is_none());
		assert!(get::<JoinLeft>(&store, OP, &other_group).is_some(), "another group must stay");
		assert!(get::<JoinLeft>(&store, neighbour, &left).is_some(), "another operator must stay");
	}

	#[test]
	fn a_row_written_in_the_same_flush_as_its_retire_does_not_survive() {
		// the write lands first, so the delete must run after it and must see a table this flush created
		let store = open();
		let key = JoinLeft::join(group(7), suffix(1));
		let mut batch = FlushBatch::default();
		batch.writes.push((OP, encode::<JoinLeft>(&key), StagedWrite::Set(row("late"))));
		batch.retired.push((OP, group(7)));

		store.flush_batch(&batch);

		assert!(get::<JoinLeft>(&store, OP, &key).is_none());
	}

	#[test]
	fn a_retire_of_a_group_with_no_table_is_harmless() {
		// a window can close having written nothing to a keyspace, which must not fail the whole flush
		let store = open();
		let mut batch = FlushBatch::default();
		batch.retired.push((OP, group(7)));

		store.flush_batch(&batch);

		assert!(scan::<JoinLeft>(&store, OP).is_empty());
	}

	#[test]
	fn a_crash_inside_the_flush_rolls_back_the_writes_and_the_retire_together() {
		// the index removal and the group delete must land as one; a half flush would strand the group forever
		let store = open();
		let doomed = JoinLeft::join(group(7), suffix(1));
		let written_late = JoinLeft::join(group(9), suffix(1));
		set_one::<JoinLeft>(&store, OP, &doomed, b"data");
		with_conn(&store, |conn| {
			conn.execute_batch(&format!(
				r#"CREATE TRIGGER crash BEFORE DELETE ON "{}" BEGIN SELECT RAISE(ABORT, 'crash'); END;"#,
				table_of(JoinLeft::NAME, OP)
			))
			.unwrap();
		});
		let mut batch = FlushBatch::default();
		batch.writes.push((OP, encode::<JoinLeft>(&written_late), StagedWrite::Set(row("new"))));
		batch.retired.push((OP, group(7)));

		let crashed = catch_unwind(AssertUnwindSafe(|| store.flush_batch(&batch)));

		assert!(crashed.is_err(), "precondition: the trigger must fail the flush, or this proves nothing");
		assert!(get::<JoinLeft>(&store, OP, &doomed).is_some(), "the group must be back after the rollback");
		assert!(
			get::<JoinLeft>(&store, OP, &written_late).is_none(),
			"the write must not land without the retire"
		);
	}
}
