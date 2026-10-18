// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::Path, sync::Arc};

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory;
use reifydb_runtime::io::fs::{Create, Open, Pwrite, memory::MemoryFs};
use reifydb_store_column::{
	block::{BlockData, ColumnChunks},
	compress::Compressor,
	convert::to_vortex,
	persist::{BlockHandle, serialize_block},
	session::new_session,
};
#[cfg(feature = "pco")]
use reifydb_store_column::{compress::CompressionProfile, convert::to_arrow};
#[cfg(feature = "pco")]
use reifydb_value::value::datetime::DateTime;
use reifydb_value::value::value_type::{ValueType, field::from_field};
use vortex_array::VortexSessionExecute;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_session::VortexSession;

type Column = (FieldRef, ArrowArrayRef);

struct SplitMix(u64);

impl SplitMix {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}
}

fn random_walk(rows: usize, seed: u64) -> Vec<i64> {
	let mut rng = SplitMix(seed);
	let mut price = 500_000i64;
	(0..rows)
		.map(|_| {
			price += (rng.next() % 41) as i64 - 20;
			price
		})
		.collect()
}

fn volumes(rows: usize, seed: u64) -> Vec<u64> {
	let mut rng = SplitMix(seed);
	(0..rows).map(|_| 100 + rng.next() % 5_000).collect()
}

fn columns(rows: usize) -> Vec<(ValueType, Column)> {
	let ts: Vec<i64> = (0..rows as i64).map(|i| 1_735_828_200_000_000_000 + i * 60_000_000_000).collect();
	vec![
		(ValueType::Int8, factory::int8("ts", ts)),
		(ValueType::Int8, factory::int8("close", random_walk(rows, 1))),
		(ValueType::Uint8, factory::uint8("volume", volumes(rows, 2))),
	]
}

fn data(compressor: &Compressor, columns: &[(ValueType, Column)]) -> BlockData {
	let schema = columns.iter().map(|(ty, column)| (column.0.name().to_string(), ty.clone(), false)).collect();
	let chunks = columns.iter().map(|(ty, column)| compressor.compress(ty.clone(), column).unwrap()).collect();
	BlockData::new(Arc::new(schema), chunks)
}

fn load(bytes: &[u8], session: &VortexSession) -> BlockData {
	let fs = MemoryFs::new();
	let path = Path::new("/block.borg");
	fs.create(path, bytes.len() as u64).unwrap().pwrite(0, bytes).unwrap();
	BlockHandle::open(fs.open(path).unwrap(), session.clone()).unwrap().read(None).unwrap()
}

#[cfg(feature = "pco")]
fn assert_round_trip(label: &str, compressor: &Compressor, columns: &[(ValueType, Column)]) {
	let bytes = serialize_block(&data(compressor, columns), &new_session()).unwrap();
	let session = new_session();
	let restored = load(&bytes, &session);
	for (_, column) in columns {
		let name = column.0.name();
		let (_, chunks) = restored.column_by_name(name).unwrap_or_else(|| panic!("{label}: {name} missing"));
		let mut offset = 0;
		for chunk in &chunks.chunks {
			let (_, read) = to_arrow(&session, name, &chunks.field_type, chunk.clone()).unwrap();
			assert_eq!(
				read.as_ref(),
				column.1.slice(offset, read.len()).as_ref(),
				"{label}: {name} differs at offset {offset}"
			);
			offset += read.len();
		}
		assert_eq!(offset, column.1.len(), "{label}: {name} lost rows");
	}
}

fn encodings(data: &BlockData) -> Vec<String> {
	data.columns
		.iter()
		.flat_map(|column| column.chunks.iter())
		.flat_map(|chunk| chunk.depth_first_traversal())
		.map(|array| array.encoding_id().to_string())
		.collect()
}

#[test]
fn the_default_profile_compresses_exactly_like_plain_btrblocks() {
	// Blocks already on disk were written by plain BtrBlocks; the default profile drifting from it would silently
	// change every new block a default build writes.
	let session = new_session();
	for (ty, column) in columns(20_000) {
		let actual = Compressor::new(session.clone()).compress(ty.clone(), &column).unwrap();
		let imported = to_vortex(&session, &column).unwrap();
		let mut ctx = session.create_execution_ctx();
		let plain = BtrBlocksCompressorBuilder::default().build().compress(&imported, &mut ctx).unwrap();
		let expected = ColumnChunks::single(ty.clone(), false, from_field(&column.0).unwrap(), plain);
		let schema = Arc::new(vec![(column.0.name().to_string(), ty, false)]);
		assert_eq!(
			serialize_block(&BlockData::new(schema.clone(), vec![actual]), &session).unwrap(),
			serialize_block(&BlockData::new(schema, vec![expected]), &session).unwrap(),
			"{}",
			column.0.name()
		);
	}
}

#[test]
fn the_default_profile_never_writes_pco() {
	// A default build cannot decode pco, so a pco array in a default-profile block would be unreadable by it.
	let session = new_session();
	let written = data(&Compressor::new(session.clone()), &columns(20_000));
	let restored = load(&serialize_block(&written, &session).unwrap(), &session);
	assert!(!encodings(&restored).iter().any(|id| id == "vortex.pco"), "{:?}", encodings(&restored));
}

#[cfg(feature = "pco")]
#[test]
fn the_compact_profile_picks_pco_for_a_random_walk() {
	// The compact profile exists to get pco onto price-like columns; if BtrBlocks never picks it the profile is
	// a no-op and every size measured with it is meaningless.
	let session = new_session();
	let written = data(&Compressor::with_profile(session.clone(), CompressionProfile::Compact), &columns(20_000));
	let restored = load(&serialize_block(&written, &session).unwrap(), &session);
	assert!(encodings(&restored).iter().any(|id| id == "vortex.pco"), "{:?}", encodings(&restored));
}

#[cfg(feature = "pco")]
#[test]
fn the_compact_profile_round_trips_losslessly() {
	// Pco is lossless only if every value, including extremes and the first and last row of each page, reads back
	// exactly; a single wrong price would corrupt OHLC data without any error.
	let compact = Compressor::with_profile(new_session(), CompressionProfile::Compact);
	let mut rows = columns(20_000);
	let mut extremes = random_walk(20_000, 3);
	extremes[0] = i64::MIN;
	extremes[8_191] = i64::MAX;
	extremes[8_192] = 0;
	rows.push((ValueType::Int8, factory::int8("extremes", extremes)));
	let mut big = volumes(20_000, 4);
	big[19_999] = u64::MAX;
	rows.push((ValueType::Uint8, factory::uint8("big", big)));
	let times: Vec<DateTime> = random_walk(20_000, 5)
		.into_iter()
		.enumerate()
		.map(|(i, jitter)| DateTime::from_nanos(1_735_828_200_000_000_000 + i as i64 * 1_000_000_000 + jitter))
		.collect();
	rows.push((ValueType::DateTime, factory::datetime("at", times)));
	assert_round_trip("compact", &compact, &rows);
	assert_round_trip("compact, small", &compact, &columns(37));
}
