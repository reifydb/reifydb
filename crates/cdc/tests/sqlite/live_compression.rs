// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_cdc::storage::{CdcStorage, sqlite::storage::SqliteCdcStorage};
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_testing::tempdir::temp_dir;
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};
use rusqlite::Connection;

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

fn compressible_cdc(version: u64) -> Cdc {
	let changes: Vec<SystemChange> = (0..64)
		.map(|i| SystemChange::Insert {
			key: EncodedKey::new(format!("repeated-key-prefix-{version}-{i}").into_bytes()),
			post: EncodedBytes(CowVec::new(vec![b'a'; 512])),
		})
		.collect();
	Cdc::new(CommitVersion(version), DateTime::from_nanos(1_700_000_000_000_000_000), Vec::new(), changes)
}

fn stored_payload(db: &std::path::Path, version: u64) -> Vec<u8> {
	let conn = Connection::open(db).unwrap();
	conn.query_row(
		r#"SELECT payload FROM "cdc" WHERE version = ?1"#,
		rusqlite::params![version.to_be_bytes().to_vec()],
		|row| row.get::<_, Vec<u8>>(0),
	)
	.unwrap()
}

fn live_row_count(db: &std::path::Path) -> i64 {
	let conn = Connection::open(db).unwrap();
	conn.query_row(r#"SELECT COUNT(*) FROM "cdc""#, [], |row| row.get(0)).unwrap()
}

#[test]
fn live_payload_is_a_zstd_frame_smaller_than_raw_postcard() {
	// The only assertion that can catch write() and the read paths both dropping compression
	// together: it inspects the bytes on disk rather than what comes back out.
	temp_dir(|path| {
		let db = path.join("cdc.reifydb");
		let cdc = compressible_cdc(1);
		let raw = postcard::to_stdvec(&cdc).unwrap();

		let store = SqliteCdcStorage::new(SqliteConfig::new(&db));
		store.write(&cdc).unwrap();
		store.shutdown();

		let stored = stored_payload(&db, 1);
		assert_eq!(&stored[..4], &ZSTD_MAGIC, "live payload must be a zstd frame, got {:x?}", &stored[..4]);
		assert!(
			stored.len() < raw.len(),
			"compression must shrink a repetitive payload: {} stored vs {} raw",
			stored.len(),
			raw.len()
		);
		Ok(())
	})
	.unwrap();
}

#[test]
fn live_row_roundtrips_without_compaction() {
	// Compaction never runs here, so this covers the live read paths alone; read() and
	// read_range() decode through different call sites and both must decompress.
	temp_dir(|path| {
		let db = path.join("cdc.reifydb");
		let store = SqliteCdcStorage::new(SqliteConfig::new(&db));
		let entries: Vec<Cdc> = (1..=32u64).map(compressible_cdc).collect();
		for cdc in &entries {
			store.write(cdc).unwrap();
		}

		for (i, cdc) in entries.iter().enumerate() {
			let got = store.read(CommitVersion(i as u64 + 1)).unwrap().expect("live entry");
			assert_eq!(postcard::to_stdvec(&got).unwrap(), postcard::to_stdvec(cdc).unwrap());
		}

		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 64).unwrap();
		assert_eq!(batch.items.len(), 32);
		assert!(!batch.has_more);
		for (got, want) in batch.items.iter().zip(entries.iter()) {
			assert_eq!(postcard::to_stdvec(got).unwrap(), postcard::to_stdvec(want).unwrap());
		}
		Ok(())
	})
	.unwrap();
}

#[test]
fn compaction_consumes_compressed_live_rows_at_its_own_level() {
	// The two zstd layers are independent: live rows are fixed at level 1, blocks take the level
	// the compactor is given. Compaction must decompress its input before packing a block.
	temp_dir(|path| {
		let db = path.join("cdc.reifydb");
		let store = SqliteCdcStorage::new(SqliteConfig::new(&db));
		let entries: Vec<Cdc> = (1..=64u64).map(compressible_cdc).collect();
		for cdc in &entries {
			store.write(cdc).unwrap();
		}
		assert_eq!(live_row_count(&db), 64);

		let summaries = store.compact_all(64, 22, CommitVersion(u64::MAX)).unwrap();
		assert_eq!(summaries.len(), 1);
		assert_eq!(summaries[0].num_entries, 64);
		assert_eq!(live_row_count(&db), 0, "compaction must delete the live rows it packed");

		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 128).unwrap();
		assert_eq!(batch.items.len(), 64);
		for (got, want) in batch.items.iter().zip(entries.iter()) {
			assert_eq!(postcard::to_stdvec(got).unwrap(), postcard::to_stdvec(want).unwrap());
		}
		Ok(())
	})
	.unwrap();
}
