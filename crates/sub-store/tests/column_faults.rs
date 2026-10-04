// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "column", reifydb_target = "host"))]

use std::{
	collections::BTreeSet,
	fs,
	path::{Path, PathBuf},
	sync::Arc,
};

use reifydb::{
	SqliteConfig, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::{execution::ExecutionResult, interface::catalog::column_snapshot::ColumnSnapshot};
use reifydb_runtime::io::fs::{
	Len, Open, Unlink,
	memory::{MemoryFs, SECTOR_BYTES},
	testing::NoFaults as FsNoFaults,
};
use reifydb_store_column::{device::BlockKey, store::ColumnStore, testing::NoFaults};
use reifydb_sub_store::subsystem::StorageConfig;
use reifydb_test_harness::fixture::column::{
	CorruptReads, LyingSync, MEMORY_ROOT, PauseOnObjectSyncDir, PauseOnRemove, SyncDirLog, TornWrites, block_path,
	host_store, memory_store,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	params::Params,
	value::{duration::Duration, identity::IdentityId},
};

fn ticking() -> StorageConfig {
	StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		..StorageConfig::default()
	}
}

fn idle() -> StorageConfig {
	StorageConfig {
		table_tick_interval: Duration::from_seconds(3600).unwrap(),
		series_tick_interval: Duration::from_seconds(3600).unwrap(),
		..StorageConfig::default()
	}
}

fn memory_db(store: ColumnStore) -> TestDb {
	TestDb::from(
		db_embedded::memory().with_storage_config(ticking()).with_column_store(store).build().expect("build"),
	)
}

fn sqlite_db(path: &Path, config: StorageConfig, store: Option<ColumnStore>) -> TestDb {
	let mut builder = db_embedded::sqlite(SqliteConfig::new(path)).with_storage_config(config);
	if let Some(store) = store {
		builder = builder.with_column_store(store);
	}
	TestDb::from(builder.build().expect("build"))
}

fn create_table(db: &TestDb) {
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4, name: utf8 }");
}

fn insert_rows(db: &TestDb, ids: impl IntoIterator<Item = u64>) {
	let rows: Vec<String> = ids
		.into_iter()
		.map(|i| {
			format!(
				"{{id: {}, name: \"{:x}\"}}",
				(i * 7919) % 100_003,
				i.wrapping_mul(2_654_435_761) % (1 << 32)
			)
		})
		.collect();
	db.command(&format!("INSERT test::t [{}]", rows.join(", ")));
}

fn table_snapshots(db: &TestDb) -> Vec<ColumnSnapshot> {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), "t").expect("find table").expect("table");
	catalog.list_column_snapshots_for_table(&mut tx, table.id).expect("list table snapshots")
}

fn table_id(db: &TestDb) -> u64 {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	catalog.find_table_by_name(&mut tx, namespace.id(), "t").expect("find table").expect("table").id.0
}

fn await_snapshot(db: &TestDb, rows: u64) -> ColumnSnapshot {
	poll_until(
		|| table_snapshots(db).into_iter().find(|snapshot| snapshot.row_count == rows),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.unwrap_or_else(|| panic!("no {rows}-row table snapshot was committed within 5 seconds"))
}

fn column_query(db: &TestDb) -> ExecutionResult {
	db.engine().query_column_as(IdentityId::root(), "from test::t", Params::None)
}

fn row_count(result: &ExecutionResult) -> usize {
	result.frames.iter().map(|frame| frame.row_count()).sum()
}

fn assert_scan_fails_with_column_error(result: &ExecutionResult, what: &str) {
	let err = result.error.as_ref().unwrap_or_else(|| {
		panic!("{what} must fail the column scan, it returned {} rows instead", row_count(result))
	});
	assert!(
		err.code.starts_with("COL_"),
		"{what} must surface the column store error, got {}: {}",
		err.code,
		err.message
	);
	assert!(result.frames.is_empty(), "no partial frames may accompany the error");
}

fn file_names(dir: &Path) -> BTreeSet<u64> {
	fs::read_dir(dir)
		.expect("list the object folder")
		.map(|entry| {
			let path = entry.expect("read folder entry").path();
			let stem = path.file_stem().expect("block file has a name").to_string_lossy().to_string();
			stem.parse()
				.unwrap_or_else(|_| panic!("unexpected file in the object folder: {}", path.display()))
		})
		.collect()
}

#[test]
fn a_torn_block_write_fails_the_column_scan_instead_of_returning_rows() {
	// Otherwise the first sector of a block reads as the whole block after a power cut mid-write.
	let memory = MemoryFs::new();
	let db = memory_db(memory_store(memory.clone(), Arc::new(TornWrites), Arc::new(NoFaults)).expect("store"));
	create_table(&db);
	insert_rows(&db, 0..2000);
	let snapshot = await_snapshot(&db, 2000);
	let path = block_path(Path::new(MEMORY_ROOT), &BlockKey::of(&snapshot));
	let len = memory.open(&path).expect("open block file").len().expect("block file length");
	assert!(len > SECTOR_BYTES as u64, "the block must span more than one sector, or the torn write loses nothing");

	assert_scan_fails_with_column_error(&column_query(&db), "a torn block");
}

#[test]
fn a_corrupt_block_read_fails_the_column_scan_instead_of_returning_rows() {
	// Otherwise flipped bits on a read decode as a block and the scan returns wrong rows.
	let db = memory_db(memory_store(MemoryFs::new(), Arc::new(CorruptReads), Arc::new(NoFaults)).expect("store"));
	create_table(&db);
	insert_rows(&db, 0..3);
	await_snapshot(&db, 3);

	assert_scan_fails_with_column_error(&column_query(&db), "a corrupt read");
}

#[test]
fn a_cataloged_block_whose_file_is_gone_fails_the_scan_instead_of_reading_empty() {
	// Otherwise a lost file looks like an empty table.
	let memory = MemoryFs::new();
	let db = memory_db(memory_store(memory.clone(), Arc::new(FsNoFaults), Arc::new(NoFaults)).expect("store"));
	create_table(&db);
	insert_rows(&db, 0..3);
	let snapshot = await_snapshot(&db, 3);
	memory.unlink(&block_path(Path::new(MEMORY_ROOT), &BlockKey::of(&snapshot))).expect("unlink block file");

	let result = column_query(&db);

	let err = result.error.as_ref().unwrap_or_else(|| {
		panic!("a missing block file must fail the scan, it returned {} rows instead", row_count(&result))
	});
	assert!(
		err.message.contains("is missing from the column store"),
		"unexpected error {}: {}",
		err.code,
		err.message
	);
	assert!(result.frames.is_empty(), "no partial frames may accompany the error");
}

#[test]
fn an_honest_sync_keeps_a_committed_block_through_a_crash() {
	// Otherwise a missing file or folder fsync drops committed column data on power loss.
	let dir = tempfile::tempdir().expect("create test dir");
	let db_path = dir.path().join("db");
	let memory = MemoryFs::new();
	{
		let store = memory_store(memory.clone(), Arc::new(FsNoFaults), Arc::new(NoFaults)).expect("store");
		let mut db = sqlite_db(&db_path, ticking(), Some(store));
		create_table(&db);
		insert_rows(&db, 0..3);
		await_snapshot(&db, 3);
		db.stop();
	}

	memory.crash();

	let store = memory_store(memory, Arc::new(FsNoFaults), Arc::new(NoFaults)).expect("store");
	let db = sqlite_db(&db_path, idle(), Some(store));
	let result = column_query(&db);
	assert!(result.error.is_none(), "a synced block must read after the crash: {:?}", result.error);
	assert_eq!(row_count(&result), 3, "every committed row must survive the crash");
}

#[test]
fn a_lying_sync_loses_the_block_and_the_scan_fails_instead_of_reading_empty() {
	// Otherwise the honest-sync test could pass because the crash dropped nothing.
	let dir = tempfile::tempdir().expect("create test dir");
	let db_path = dir.path().join("db");
	let memory = MemoryFs::new();
	{
		let store = memory_store(memory.clone(), Arc::new(LyingSync), Arc::new(NoFaults)).expect("store");
		let mut db = sqlite_db(&db_path, ticking(), Some(store));
		create_table(&db);
		insert_rows(&db, 0..3);
		await_snapshot(&db, 3);
		db.stop();
	}

	memory.crash();

	let store = memory_store(memory, Arc::new(FsNoFaults), Arc::new(NoFaults)).expect("store");
	let db = sqlite_db(&db_path, idle(), Some(store));
	assert_scan_fails_with_column_error(&column_query(&db), "a block whose sync lied");
}

#[test]
fn a_delete_paused_after_the_catalog_commit_leaves_an_orphan_and_the_latest_reads() {
	// Otherwise a crash at the old-file delete can lose the latest snapshot or its file.
	let dir = tempfile::tempdir().expect("create test dir");
	let root = dir.path().join("column");
	let (hook, pause) = PauseOnRemove::new();
	let hook = Arc::new(hook);
	let db = memory_db(host_store(root.clone(), hook.clone()).expect("store"));
	create_table(&db);
	hook.target(table_id(&db));
	insert_rows(&db, 0..1);
	let first = await_snapshot(&db, 1);
	insert_rows(&db, 1..2);

	let removing =
		pause.reached(Duration::from_seconds(5).unwrap()).expect("the old file delete was never reached");

	assert_eq!(removing, first.id.0, "the delete must target the previous snapshot");
	let snapshots = table_snapshots(&db);
	assert_eq!(snapshots.len(), 1, "the catalog must already have dropped the old row when its file is deleted");
	assert_eq!(snapshots[0].row_count, 2, "the committed row must be the latest snapshot");
	let latest = BlockKey::of(&snapshots[0]);
	let folder = root.join(latest.dir.to_string());
	assert_eq!(
		file_names(&folder),
		BTreeSet::from([first.id.0, latest.name]),
		"a crash here must leave the old file as an orphan next to the latest"
	);
	let restarted = ColumnStore::host(root.clone()).expect("reopen column dir");
	let block = restarted
		.open(&latest)
		.expect("open latest block")
		.expect("the latest snapshot must have its file on disk")
		.read(None)
		.expect("read latest block");
	assert_eq!(block.len(), 2, "a fresh store over the same folder must read the latest block whole");

	pause.release();
	poll_until(
		|| (file_names(&folder) == BTreeSet::from([latest.name])).then_some(()),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("the old file must be deleted once the delete runs");
}

#[test]
fn the_snapshot_row_stays_invisible_until_its_object_folder_is_synced() {
	// Otherwise a catalog row can point at a file whose folder entry a crash would drop.
	let (hook, pause) = PauseOnObjectSyncDir::new();
	let hook = Arc::new(hook);
	let db = memory_db(memory_store(MemoryFs::new(), hook.clone(), Arc::new(NoFaults)).expect("store"));
	create_table(&db);
	let table = table_id(&db);
	hook.target(table);
	insert_rows(&db, 0..3);

	let syncing =
		pause.reached(Duration::from_seconds(5).unwrap()).expect("the object folder sync was never reached");

	assert_eq!(syncing, PathBuf::from(MEMORY_ROOT).join(table.to_string()));
	assert!(table_snapshots(&db).is_empty(), "no snapshot row may be visible before its folder is synced");
	pause.release();
	await_snapshot(&db, 3);
}

#[test]
fn the_root_is_synced_once_per_new_object_folder_and_the_object_folder_once_per_snapshot() {
	// Otherwise a new folder entry is never made durable and a crash drops it with its files.
	let log = Arc::new(SyncDirLog::default());
	let store = memory_store(MemoryFs::new(), log.clone(), Arc::new(NoFaults)).expect("store");
	assert_eq!(log.paths(), vec![PathBuf::from("/")], "creating the root must sync its parent");

	let db = memory_db(store);
	create_table(&db);
	let object = PathBuf::from(MEMORY_ROOT).join(table_id(&db).to_string());
	insert_rows(&db, 0..1);
	await_snapshot(&db, 1);
	assert_folder_syncs(&log.paths(), &object, 1);

	insert_rows(&db, 1..2);
	await_snapshot(&db, 2);
	assert_folder_syncs(&log.paths(), &object, 2);
}

fn assert_folder_syncs(paths: &[PathBuf], object: &Path, snapshots: usize) {
	let root = Path::new(MEMORY_ROOT);
	let folders: BTreeSet<&PathBuf> = paths.iter().filter(|path| path.parent() == Some(root)).collect();
	assert!(folders.contains(&object.to_path_buf()), "the table's object folder must be synced: {paths:?}");
	assert_eq!(
		paths.iter().filter(|path| path.as_path() == root).count(),
		folders.len(),
		"the root must be synced exactly once per new object folder: {paths:?}"
	);
	assert_eq!(
		paths.iter().filter(|path| path.as_path() == object).count(),
		snapshots,
		"the object folder must be synced once per snapshot: {paths:?}"
	);
}
