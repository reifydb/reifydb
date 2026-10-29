// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	fs,
	io::ErrorKind,
	path::{Path, PathBuf},
	sync::Arc,
	thread::sleep,
};

use reifydb::{
	Clock, Database, Error, IdentityId, SqliteConfig,
	arrow_array::{
		Array, ArrayRef,
		cast::AsArray,
		types::{Int64Type, TimestampNanosecondType, UInt32Type, UInt64Type},
	},
	arrow_schema::{DataType, TimeUnit},
	column::{
		block::BlockData, compress::CompressionProfile, convert::to_arrow, device::BlockFileKey,
		persist::serialize_block, store::ColumnStore,
	},
	core::{
		interface::catalog::column_block::{ColumnBlock, ColumnBlockSource},
		value::batch::batch,
	},
	embedded,
	sub_store::subsystem::StorageConfig,
	transaction::transaction::Transaction,
	value::{params::Params, value::duration::Duration},
};

use crate::{
	data::{Column, Dataset, RowRef, Variant, columns},
	encoding::{OHLCV, OhlcvEncoding},
	profile_name,
	report::{ColumnTally, Row},
	transform::{Derived, inverse},
};

pub struct Run {
	pub name: &'static str,
	pub series: &'static str,
	pub ddl: &'static str,
	pub variant: Variant,
	pub with_pool: bool,
	pub width: u64,
	pub layout: &'static str,
	pub tick_ms: u64,
	pub txn_rows: usize,
	pub encoded: bool,
	pub merge_buckets: u64,
}

const INSERT_CHUNK: usize = 50_000;

fn err(e: Error) -> String {
	e.to_string()
}

pub fn root() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/ohlcv-bench")
}

pub fn insert_order(ds: &Dataset) -> Vec<RowRef> {
	let mut rows: Vec<RowRef> = ds
		.series
		.iter()
		.enumerate()
		.flat_map(|(s, series)| (0..series.key.len()).map(move |i| (s, i)))
		.collect();
	rows.sort_by_key(|&(s, i)| (ds.series[s].key[i], ds.series[s].pool));
	rows
}

fn as_i128(array: &ArrayRef) -> Result<Vec<i128>, String> {
	Ok(match array.data_type() {
		DataType::Int64 => array.as_primitive::<Int64Type>().values().iter().map(|&v| v as i128).collect(),
		DataType::UInt64 => array.as_primitive::<UInt64Type>().values().iter().map(|&v| v as i128).collect(),
		DataType::UInt32 => array.as_primitive::<UInt32Type>().values().iter().map(|&v| v as i128).collect(),
		DataType::Timestamp(TimeUnit::Nanosecond, _) => {
			array.as_primitive::<TimestampNanosecondType>().values().iter().map(|&v| v as i128).collect()
		}
		other => return Err(format!("unexpected column type {other}")),
	})
}

fn to_rows(columns: &[Vec<i128>]) -> Vec<Box<[i128]>> {
	let len = columns.first().map_or(0, Vec::len);
	let mut rows: Vec<Box<[i128]>> = (0..len).map(|i| columns.iter().map(|c| c[i]).collect()).collect();
	rows.sort_unstable();
	rows
}

fn series_blocks(db: &Database, run: &Run) -> Result<Vec<ColumnBlock>, String> {
	let (namespace, name) = run.series.split_once("::").ok_or("series name has no namespace")?;
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).map_err(err)?;
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, namespace).map_err(err)?.ok_or("no namespace")?;
	let series = catalog.find_series_by_name(&mut tx, namespace.id(), name).map_err(err)?.ok_or("no series")?;
	catalog.list_column_blocks_for_series(&mut tx, series.id).map_err(err)
}

fn settled(blocks: &[ColumnBlock], window: u64) -> bool {
	let mut per_window: BTreeMap<(Option<u128>, u64), usize> = BTreeMap::new();
	for block in blocks {
		if let ColumnBlockSource::Series {
			partition,
			start,
			..
		} = block.source
		{
			*per_window.entry((partition.map(|p| p.0), start - start % window)).or_default() += 1;
		}
	}
	per_window.values().all(|count| *count == 1)
}

fn await_settled(db: &Database, run: &Run, expected: usize) -> Result<(usize, Vec<ColumnBlock>), String> {
	let poll = Duration::from_milliseconds(250).map_err(|e| e.to_string())?.to_std();
	let no_progress_limit =
		Duration::from_milliseconds(run.tick_ms as i64 + 60_000).map_err(|e| e.to_string())?.to_std();
	let window = run.width.saturating_mul(run.merge_buckets.max(1));
	let clock = Clock::Real;
	let mut last = (0, 0);
	let mut since_progress = clock.instant();
	loop {
		let blocks = series_blocks(db, run)?;
		let total = blocks.iter().map(|block| block.row_count as usize).sum::<usize>();
		if total >= expected && (run.merge_buckets == 0 || settled(&blocks, window)) {
			return Ok((total, blocks));
		}
		if (total, blocks.len()) != last {
			last = (total, blocks.len());
			since_progress = clock.instant();
		}
		if since_progress.elapsed() > no_progress_limit {
			return Err(format!(
				"{} stopped settling at {total}/{expected} rows in {} blocks",
				run.name,
				blocks.len()
			));
		}
		sleep(poll);
	}
}

fn block_file(column_dir: &Path, key: &BlockFileKey) -> PathBuf {
	column_dir.join(key.dir.to_string()).join(format!("{:020}.borg", key.name))
}

fn file_bytes(path: &Path) -> Result<u64, String> {
	fs::metadata(path).map(|meta| meta.len()).map_err(|e| format!("{}: {e}", path.display()))
}

fn row_store_bytes(dir: &Path) -> Result<u64, String> {
	let wal = match fs::metadata(dir.join("multi.db-wal")) {
		Ok(meta) => meta.len(),
		Err(e) if e.kind() == ErrorKind::NotFound => 0,
		Err(e) => return Err(e.to_string()),
	};
	Ok(file_bytes(&dir.join("multi.db"))? + wal)
}

fn column_read(db: &Database, run: &Run, expected: &[Column]) -> Result<Option<Vec<Vec<i128>>>, String> {
	let frames = match db.query_column_as_root(&format!("FROM {}", run.series), Params::None) {
		Ok(frames) => frames,
		Err(e) if e.code == "QUERY_012" => return Ok(None),
		Err(e) => return Err(err(e)),
	};
	let mut read: Vec<Vec<i128>> = vec![Vec::new(); expected.len()];
	for frame in &frames {
		for (c, (field, _)) in expected.iter().enumerate() {
			let array = frame
				.batch
				.column_by_name(field.name())
				.ok_or_else(|| format!("column read has no {}", field.name()))?;
			read[c].extend(as_i128(array)?);
		}
	}
	Ok(Some(read))
}

fn check_column_read(db: &Database, run: &Run, expected: &[Column]) -> Result<(), String> {
	let inserted = to_rows(&expected.iter().map(|c| as_i128(&c.1)).collect::<Result<Vec<_>, _>>()?);
	let poll = Duration::from_milliseconds(250).map_err(|e| e.to_string())?.to_std();
	let visible_limit = Duration::from_milliseconds(60_000).map_err(|e| e.to_string())?.to_std();
	let clock = Clock::Real;
	let start = clock.instant();
	loop {
		let read = column_read(db, run, expected)?;
		let visible = read.as_ref().map_or(0, |read| read[0].len());
		if visible < inserted.len() && start.elapsed() <= visible_limit {
			sleep(poll);
			continue;
		}
		let Some(read) = read else {
			return Err("query_column found no materialized block".to_string());
		};
		if to_rows(&read) != inserted {
			return Err(format!(
				"query_column rows differ from inserted ({} vs {})",
				read[0].len(),
				inserted.len()
			));
		}
		return Ok(());
	}
}

fn check_inverse(restored: &[Box<[i128]>], ds: &Dataset) -> Result<(), String> {
	let series = &ds.series[0];
	let at = |row: &[i128], c: usize| row[c] as i64;
	let derived = Derived {
		close: restored.iter().map(|r| at(r, 1)).collect(),
		gap: restored.iter().map(|r| at(r, 2)).collect(),
		upper: restored.iter().map(|r| r[3] as u64).collect(),
		lower: restored.iter().map(|r| r[4] as u64).collect(),
	};
	if inverse(&derived, &series.segments)? != series.ohlc {
		return Err("inverse transform of stored rows differs".to_string());
	}
	Ok(())
}

pub fn measure(ds: &Dataset, run: &Run, profile: CompressionProfile) -> Result<Row, String> {
	let dir = root().join(format!("{}-{}", run.name, profile_name(profile)));
	if dir.exists() {
		fs::remove_dir_all(&dir).map_err(|e| e.to_string())?;
	}
	fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
	let column_dir = dir.join("bench").join("column");

	let mut db = embedded::sqlite(SqliteConfig::new(dir.join("bench.db")))
		.with_storage_config(StorageConfig {
			series_bucket_width: run.width,
			series_grace: Duration::from_seconds(1).map_err(|e| e.to_string())?,
			series_tick_interval: Duration::from_milliseconds(run.tick_ms as i64)
				.map_err(|e| e.to_string())?,
			series_merge_buckets: run.merge_buckets,
			compression: profile,
			materialize: true,
			..StorageConfig::default()
		})
		.with_block_encodings(|c| c.register(OHLCV, || OhlcvEncoding))
		.build()
		.map_err(err)?;
	db.admin_as_root("CREATE NAMESPACE bench", Params::None).map_err(|e| format!("create namespace: {e}"))?;
	db.admin_as_root(run.ddl, Params::None).map_err(|e| format!("create series: {e}"))?;

	let order = insert_order(ds);
	let clock = Clock::Real;
	let start = clock.instant();
	let batches = order
		.chunks(INSERT_CHUNK)
		.map(|chunk| batch(columns(ds, chunk, run.variant, run.with_pool)).map_err(err))
		.collect::<Result<Vec<_>, String>>()?;
	for (n, txn) in batches.chunks(run.txn_rows.div_ceil(INSERT_CHUNK)).enumerate() {
		let mut bulk = db.engine().bulk_insert(IdentityId::root());
		let mut series = bulk.series(run.series);
		for record_batch in txn {
			series = series.batch(record_batch.clone());
		}
		series.done();
		bulk.execute().map_err(|e| format!("bulk insert txn {n}: {e}"))?;
	}
	let insert_secs = start.elapsed().as_secs_f64();

	let expected = columns(ds, &order, run.variant, run.with_pool);
	let names: Vec<String> = expected.iter().map(|c| c.0.name().to_string()).collect();
	let start = clock.instant();
	let (materialized, live) = await_settled(&db, run, order.len())?;
	let seal_secs = start.elapsed().as_secs_f64();
	let mut verified = check_column_read(&db, run, &expected);
	db.stop().map_err(|e| format!("stop: {e}"))?;
	let row_store_bytes = row_store_bytes(&dir.join("bench"))?;

	let store = ColumnStore::host(column_dir.clone()).map_err(err)?;

	let session = store.session().clone();
	let mut tally = ColumnTally::default();
	let mut bytes = 0;
	let mut system_bytes = 0;
	let mut blocks = 0;
	let mut empty_blocks = 0;
	let mut stored: Vec<Vec<i128>> = vec![Vec::new(); names.len()];
	let mut all_names: Vec<String> = Vec::new();
	for block in &live {
		let key = BlockFileKey::of(block);
		let size = file_bytes(&block_file(&column_dir, &key))?;
		let data = store.open(&key).map_err(err)?.ok_or("block vanished")?.read(None).map_err(err)?;
		if data.column_by_name(&names[0]).is_none() {
			continue;
		}
		blocks += 1;
		if data.is_empty() {
			empty_blocks += 1;
		}
		bytes += size;
		for ((name, ty, nullable), column) in data.schema.iter().zip(&data.columns) {
			let single = BlockData::new(
				Arc::new(vec![(name.clone(), ty.clone(), *nullable)]),
				vec![column.clone()],
			);
			let single_bytes = serialize_block(&single, &session).map_err(err)?.len() as u64;
			let encoding = column.chunks.first().map(|c| c.encoding_id().to_string()).unwrap_or_default();
			tally.add(name, single_bytes, encoding);
			if name.starts_with('#') {
				system_bytes += single_bytes;
			}
			if !all_names.contains(name) {
				all_names.push(name.clone());
			}
			if !run.encoded
				&& let Some(c) = names.iter().position(|n| n == name)
			{
				for chunk in &column.chunks {
					let (_, array) = to_arrow(&session, name, &column.field_type, chunk.clone())
						.map_err(err)?;
					stored[c].extend(as_i128(&array)?);
				}
			}
		}
	}

	println!(
		"phase2 {} {}: insert {insert_secs:.1}s, materialize {seal_secs:.1}s, {materialized}/{} rows, {blocks} blocks ({empty_blocks} empty), merge window {} buckets, row store {row_store_bytes} bytes",
		run.name,
		profile_name(profile),
		order.len(),
		run.merge_buckets
	);

	let restored = if run.encoded {
		Vec::new()
	} else {
		to_rows(&stored)
	};
	let inserted = to_rows(&expected.iter().map(|c| as_i128(&c.1)).collect::<Result<Vec<_>, _>>()?);
	if verified.is_ok() && !run.encoded && restored != inserted {
		verified = Err(format!("stored rows differ from inserted ({} vs {})", restored.len(), inserted.len()));
	}
	if verified.is_ok() && run.variant == Variant::Derived {
		verified = check_inverse(&restored, ds);
	}

	Ok(Row {
		phase: "2",
		dataset: ds.name.to_string(),
		layout: run.layout.to_string(),
		columns: format!(
			"{} {}",
			run.name,
			if run.encoded {
				"ohlcv encoded"
			} else {
				run.variant.label()
			}
		),
		profile: profile_name(profile).to_string(),
		blocks,
		rows: order.len(),
		bytes,
		system_bytes: Some(system_bytes),
		decode_mbps: None,
		verified: match verified {
			Ok(()) => "yes".to_string(),
			Err(e) => format!("NO: {e}"),
		},
		per_column: tally.finish(&all_names),
	})
}
