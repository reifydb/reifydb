// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	interface::catalog::{id::SeriesId, series::SeriesPartitionMetadata},
	key::{any::TaggedKey, series::SeriesPartitionMetadataKey},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{
	multi::transaction::write::Rebase,
	transaction::{Transaction, command::CommandTransaction},
};
use reifydb_value::{params, util::cowvec::CowVec, value::partition::Partition};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { k: int8, v: int4 } WITH { key: k }");
	t.command("INSERT test::s [{ k: 1, v: 10 }, { k: 2, v: 20 }, { k: 3, v: 30 }]");
	t
}

fn metadata(t: &TestEngine) -> SeriesPartitionMetadata {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	let series = catalog.find_series_by_name(&mut txn, namespace.id(), "s").unwrap().unwrap();
	catalog.find_series_metadata(&mut txn, series.id, Partition::default()).unwrap().unwrap()
}

fn open(t: &TestEngine, rql: &[&str]) -> CommandTransaction {
	let mut txn = t.begin_command(TestEngine::identity()).expect("begin command");
	for statement in rql {
		if let Some(error) = txn.rql(statement, params::Params::None).error {
			panic!("{statement} failed inside its transaction: {error:?}");
		}
	}
	txn
}

fn unchecked_bulk(t: &TestEngine) {
	let mut builder = t.bulk_insert_unchecked(TestEngine::identity());
	builder.series("test::s")
		.rows([params! { k: 4, v: 40 }, params! { k: 5, v: 50 }, params! { k: 6, v: 60 }])
		.done();
	builder.execute().expect("the unchecked bulk commits");
}

#[test]
fn a_delete_racing_an_unchecked_bulk_keeps_the_bulk_rows_in_the_metadata() {
	// The bulk committed after the delete read the metadata; the delete may only take away the rows it
	// removed, or the counts, the key range and the write time drift from what is stored.
	let t = engine();
	let mut delete = open(&t, &["DELETE test::s FILTER { k == 2 }"]);
	unchecked_bulk(&t);
	let bulk = metadata(&t);

	delete.commit().expect("the delete commits");

	let after = metadata(&t);
	assert_eq!(after.row_count, 5);
	assert_eq!(after.sequence_counter, 6);
	assert_eq!((after.oldest_key, after.newest_key), (1, 6));
	assert_eq!(after.last_write_at, bulk.last_write_at);
	assert_eq!((after.dirty_from_key, after.dirty_to_key), (1, 7));
}

#[test]
fn a_delete_that_empties_its_snapshot_racing_an_unchecked_bulk_keeps_the_bulk_key_range() {
	// The delete saw every row go, but the bulk rows are still stored; resetting the key range would let a
	// generated key restart below them.
	let t = engine();
	let mut delete = open(&t, &["DELETE test::s FILTER { k <= 3 }"]);
	unchecked_bulk(&t);

	delete.commit().expect("the delete commits");

	let after = metadata(&t);
	assert_eq!(after.row_count, 3);
	assert_eq!(after.sequence_counter, 6);
	assert_eq!((after.oldest_key, after.newest_key), (1, 6));
}

#[test]
fn a_delete_that_empties_the_series_resets_its_key_range_and_keeps_its_counter() {
	// Row numbers are never reused, so emptying the series clears the key range but not the counter.
	let t = engine();

	t.command("DELETE test::s FILTER { k <= 3 }");

	let after = metadata(&t);
	assert_eq!(after.row_count, 0);
	assert_eq!((after.oldest_key, after.newest_key), (0, 0));
	assert_eq!(after.sequence_counter, 3);
	assert_eq!((after.dirty_from_key, after.dirty_to_key), (1, 4));
}

#[test]
fn two_deletes_in_one_transaction_racing_an_unchecked_bulk_both_count() {
	// Each delete statement takes its own rows away; replaying only the last one at commit would leave a
	// deleted row counted.
	let t = engine();
	let mut delete = open(&t, &["DELETE test::s FILTER { k == 1 }", "DELETE test::s FILTER { k == 2 }"]);
	unchecked_bulk(&t);

	delete.commit().expect("the delete commits");

	let after = metadata(&t);
	assert_eq!(after.row_count, 4);
	assert_eq!(after.sequence_counter, 6);
}

#[test]
fn an_insert_after_a_delete_in_one_transaction_keeps_its_row_number() {
	// Replaying only the delete at commit would drop the insert's counter bump and hand its row number out
	// again.
	let t = engine();
	let mut txn = open(&t, &["DELETE test::s FILTER { k == 2 }", "INSERT test::s [{ k: 8, v: 80 }]"]);

	txn.commit().expect("the transaction commits");

	let after = metadata(&t);
	assert_eq!(after.row_count, 3);
	assert_eq!(after.sequence_counter, 4);
}

#[test]
fn a_delete_after_an_insert_in_one_transaction_keeps_the_insert_row_number() {
	// The delete read metadata the insert already changed; replaying the delete alone on the stored metadata
	// would forget the insert.
	let t = engine();
	let mut txn = open(&t, &["INSERT test::s [{ k: 8, v: 80 }]", "DELETE test::s FILTER { k == 2 }"]);

	txn.commit().expect("the transaction commits");

	let after = metadata(&t);
	assert_eq!(after.row_count, 3);
	assert_eq!(after.sequence_counter, 4);
}

#[test]
fn a_delete_racing_a_checked_insert_still_conflicts() {
	// Only unchecked commits bypass conflict detection; a checked writer of the same metadata must still fail
	// the later commit instead of being merged.
	let t = engine();
	let mut delete = open(&t, &["DELETE test::s FILTER { k == 2 }"]);
	t.command("INSERT test::s [{ k: 4, v: 40 }]");

	let error = delete.commit().expect_err("the delete conflicts");

	assert_eq!(error.0.code, "TXN_001");
	let after = metadata(&t);
	assert_eq!(after.row_count, 4);
	assert_eq!(after.sequence_counter, 4);
}

#[test]
fn an_admin_delete_racing_an_unchecked_bulk_keeps_the_bulk_rows_in_the_metadata() {
	// A delete run inside an admin transaction writes the same metadata and commits the same way; it must not
	// rewind the counter either.
	let t = engine();
	let mut delete = t.inner().begin_admin(TestEngine::identity()).expect("begin admin");
	if let Some(error) = delete.rql("DELETE test::s FILTER { k == 2 }", params::Params::None).error {
		panic!("the delete failed inside its transaction: {error:?}");
	}
	unchecked_bulk(&t);

	delete.commit().expect("the delete commits");

	let after = metadata(&t);
	assert_eq!(after.row_count, 5);
	assert_eq!(after.sequence_counter, 6);
}

#[test]
fn a_rebased_write_on_a_finished_transaction_is_refused() {
	// A finished transaction has no writer left; the write must fail like any other instead of panicking.
	let t = engine();
	let key: TaggedKey = SeriesPartitionMetadataKey::new(SeriesId(1), Partition::default()).into();
	let bytes = EncodedBytes(CowVec::new(vec![0]));
	let rebase: Rebase = Arc::new(|bytes: &EncodedBytes| Ok(bytes.clone()));
	let mut command = t.begin_command(TestEngine::identity()).expect("begin command");
	command.commit().expect("the empty command commits");
	let mut admin = t.inner().begin_admin(TestEngine::identity()).expect("begin admin");
	admin.commit().expect("the empty admin commits");

	let refused = [
		command.set_rebased(&key, bytes.clone(), rebase.clone()).expect_err("the command refuses"),
		admin.set_rebased(&key, bytes, rebase).expect_err("the admin refuses"),
	];

	assert_eq!(refused.map(|error| error.0.code), ["TXN_008".to_string(), "TXN_008".to_string()]);
}
