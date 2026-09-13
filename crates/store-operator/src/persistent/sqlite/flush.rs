// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::{KeyspaceVisitor, dispatch},
		state::OperatorStateKey,
		traits::Keyspace,
	},
};
use rusqlite::{Transaction, TransactionBehavior, params};
use tracing::instrument;

use crate::{
	persistent::sqlite::{
		SqlitePersistent, route,
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
					route::drop_operator(&transaction, *operator);
				}
			}
		}

		write_state(&state_of(batch), &transaction);

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

fn write_state(state: &BucketMap, txn: &Transaction) {
	for ((_, keyspace), bucket) in state.buckets() {
		dispatch(
			*keyspace,
			Write {
				bucket,
				txn,
			},
		)
		.expect("a bucketed keyspace must be in the catalogue");
	}
}

struct Write<'a> {
	bucket: &'a dyn Bucket,
	txn: &'a Transaction<'a>,
}

impl KeyspaceVisitor for Write<'_> {
	type Output = ();

	fn visit<K: Keyspace>(self) -> Self::Output {
		let bucket = self
			.bucket
			.as_any()
			.downcast_ref::<StandardBucket<K>>()
			.expect("a keyspace id must map to exactly one key type");
		let mut sets: Vec<(OperatorId, K::GroupedKey, Vec<u8>)> = Vec::new();
		let mut removes: Vec<(OperatorId, K::GroupedKey)> = Vec::new();
		for (group, suffix, entry) in bucket.entries() {
			let key = K::join(group, suffix.clone());
			match &entry.post {
				Some(row) => sets.push((bucket.operator(), key, row.as_slice().to_vec())),
				None => removes.push((bucket.operator(), key)),
			}
		}
		typed::set_chunked::<K>(self.txn, &sets);
		typed::remove_chunked::<K>(self.txn, &removes);
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
	use rusqlite::Connection;

	use crate::{
		persistent::sqlite::{flush::write_state, schema::ensure_schema, typed},
		resident::bucket::BucketMap,
	};

	const OP: OperatorId = OperatorId(1);

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn suffix(n: u64) -> Asc<RowNumber> {
		Asc(RowNumber(n))
	}

	fn write(conn: &Connection, map: &BucketMap) {
		let txn = conn.unchecked_transaction().expect("begin");
		write_state(map, &txn);
		txn.commit().expect("commit");
	}

	#[test]
	fn a_flush_writes_every_group_into_the_keyspaces_own_table() {
		let conn = Connection::open_in_memory().expect("in memory db");
		ensure_schema(&conn);

		let mut map = BucketMap::default();
		map.bucket::<JoinLeft>(OP).record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("seven")));
		map.bucket::<JoinLeft>(OP).record(GroupId::hashed(Hash128(9)), suffix(2), Some(row("nine")));
		write(&conn, &map);

		let rows = typed::scan::<JoinLeft>(&conn, OP);
		assert_eq!(rows.len(), 2, "every group in the bucket must reach the table, not just the first");
	}

	#[test]
	fn a_flushed_tombstone_deletes_the_row_rather_than_storing_a_none() {
		let conn = Connection::open_in_memory().expect("in memory db");
		ensure_schema(&conn);

		let mut map = BucketMap::default();
		map.bucket::<JoinLeft>(OP).record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("live")));
		write(&conn, &map);

		map.bucket::<JoinLeft>(OP).record(GroupId::hashed(Hash128(7)), suffix(1), None);
		write(&conn, &map);

		assert!(
			typed::scan::<JoinLeft>(&conn, OP).is_empty(),
			"a removal must delete the durable row; leaving it behind resurrects state the operator deleted"
		);
	}

	#[test]
	fn a_flushed_row_survives_the_round_trip_through_its_payload() {
		let conn = Connection::open_in_memory().expect("in memory db");
		ensure_schema(&conn);

		let mut map = BucketMap::default();
		map.bucket::<JoinLeft>(OP).record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("payload")));
		write(&conn, &map);

		let stored = typed::get::<JoinLeft>(&conn, OP, &JoinLeft::join(GroupId::hashed(Hash128(7)), suffix(1)))
			.expect("the row");
		let restored = EncodedPodRow::from(EncodedBytes(CowVec::new(stored)));
		assert_eq!(
			String::from_utf8(restored.body().to_vec()).expect("utf8"),
			"payload",
			"the payload round trip carries the pod header, so reading back the body alone would truncate the row"
		);
	}
}
