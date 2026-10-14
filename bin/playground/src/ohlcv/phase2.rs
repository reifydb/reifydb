// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	fs,
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
		compress::CompressionProfile, convert::to_arrow, device::BlockKey, persist::serialize_block,
		snapshot::ColumnBlock, store::ColumnStore,
	},
	core::value::batch::batch,
	embedded,
	sub_store::subsystem::StorageConfig,
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
}

const INSERT_CHUNK: usize = 50_000;
const MAX_ATTEMPTS: usize = 5;

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

fn block_files(dir: &Path) -> Result<Vec<(BlockKey, PathBuf, u64)>, String> {
	let mut out = Vec::new();
	for series_dir in fs::read_dir(dir).map_err(|e| e.to_string())? {
		let series_dir = series_dir.map_err(|e| e.to_string())?;
		let Some(series) = series_dir.file_name().to_str().and_then(|n| n.parse::<u64>().ok()) else {
			continue;
		};
		for file in fs::read_dir(series_dir.path()).map_err(|e| e.to_string())? {
			let file = file.map_err(|e| e.to_string())?;
			let path = file.path();
			let Some(name) = path
				.extension()
				.filter(|e| *e == "borg")
				.and(path.file_stem())
				.and_then(|s| s.to_str())
				.and_then(|s| s.parse::<u64>().ok())
			else {
				continue;
			};
			let size = file.metadata().map_err(|e| e.to_string())?.len();
			out.push((
				BlockKey {
					dir: series,
					name,
				},
				path,
				size,
			));
		}
	}
	out.sort_by(|a, b| a.1.cmp(&b.1));
	Ok(out)
}

fn await_blocks(store: &ColumnStore, dir: &Path, key: &str, expected: usize, tick_ms: u64) -> Result<usize, String> {
	let poll = Duration::from_milliseconds(250).map_err(|e| e.to_string())?.to_std();
	let no_progress_limit =
		Duration::from_milliseconds(tick_ms as i64 + 60_000).map_err(|e| e.to_string())?.to_std();
	let clock = Clock::Real;
	let mut counted: HashMap<PathBuf, (u64, usize)> = HashMap::new();
	let mut last_total = 0;
	let mut since_progress = clock.instant();
	loop {
		let mut total = 0;
		for (block_key, path, size) in block_files(dir)? {
			let rows = match counted.get(&path) {
				Some(&(seen, rows)) if seen == size => rows,
				_ => {
					let Some(handle) = store.open(&block_key).map_err(err)? else {
						continue;
					};
					let Ok(block) = handle.read(Some(&[key])) else {
						continue;
					};
					counted.insert(path, (size, block.len()));
					block.len()
				}
			};
			total += rows;
		}
		if total >= expected {
			return Ok(total);
		}
		if total != last_total {
			last_total = total;
			since_progress = clock.instant();
		}
		if since_progress.elapsed() > no_progress_limit {
			return Ok(total);
		}
		sleep(poll);
	}
}

fn check_aggregate(db: &Database, run: &Run, expected: &[Column]) -> Result<(), String> {
	let close = expected.iter().find(|c| c.0.name() == "close").ok_or("no close column")?;
	let close = as_i128(&close.1)?;
	let (n, sum) = (close.len(), close.iter().sum::<i128>());
	let rql = format!("FROM {} AGGREGATE {{ n: math::count(close), s: math::sum(close) }}", run.series);
	let frames = db.query_as_root(&rql, Params::None).map_err(err)?;
	let row = frames.first().and_then(|f| f.to_rows().into_iter().next()).ok_or("aggregate returned no row")?;
	let value = |name: &str| row.iter().find(|(n, _)| n == name).map(|(_, v)| v.to_string()).unwrap_or_default();
	if value("n") != n.to_string() || value("s") != sum.to_string() {
		return Err(format!("aggregate n={} s={} expected n={n} s={sum}", value("n"), value("s")));
	}
	Ok(())
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
			compression: profile,
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
	let mut conflicts = 0;
	let batches = order
		.chunks(INSERT_CHUNK)
		.map(|chunk| batch(columns(ds, chunk, run.variant, run.with_pool)).map_err(err))
		.collect::<Result<Vec<_>, String>>()?;
	for (n, txn) in batches.chunks(run.txn_rows.div_ceil(INSERT_CHUNK)).enumerate() {
		let mut attempt = 1;
		loop {
			let mut bulk = db.engine().bulk_insert(IdentityId::root());
			let mut series = bulk.series(run.series);
			for record_batch in txn {
				series = series.batch(record_batch.clone());
			}
			series.done();
			match bulk.execute() {
				Ok(_) => break,
				Err(e) if e.0.code == "TXN_001" && attempt < MAX_ATTEMPTS => {
					conflicts += 1;
					attempt += 1;
				}
				Err(e) => return Err(format!("bulk insert txn {n} (attempt {attempt}): {e}")),
			}
		}
	}
	let insert_secs = start.elapsed().as_secs_f64();

	let expected = columns(ds, &order, run.variant, run.with_pool);
	let names: Vec<String> = expected.iter().map(|c| c.0.name().to_string()).collect();
	let mut verified = check_aggregate(&db, run, &expected);

	let store = ColumnStore::host(column_dir.clone()).map_err(err)?;
	let start = clock.instant();
	let materialized = await_blocks(&store, &column_dir, &names[0], order.len(), run.tick_ms)?;
	let seal_secs = start.elapsed().as_secs_f64();
	if verified.is_ok() {
		verified = check_column_read(&db, run, &expected);
	}
	db.stop().map_err(|e| format!("stop: {e}"))?;

	let session = store.session().clone();
	let mut tally = ColumnTally::default();
	let mut bytes = 0;
	let mut system_bytes = 0;
	let mut blocks = 0;
	let mut empty_blocks = 0;
	let mut stored: Vec<Vec<i128>> = vec![Vec::new(); names.len()];
	let mut all_names: Vec<String> = Vec::new();
	for (key, _, size) in block_files(&column_dir)? {
		let block = store.open(&key).map_err(err)?.ok_or("block vanished")?.read(None).map_err(err)?;
		if block.column_by_name(&names[0]).is_none() {
			continue;
		}
		blocks += 1;
		if block.is_empty() {
			empty_blocks += 1;
		}
		bytes += size;
		for ((name, ty, nullable), column) in block.schema.iter().zip(&block.columns) {
			let single = ColumnBlock::new(
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
		"phase2 {} {}: insert {insert_secs:.1}s ({conflicts} TXN_001 retries), materialize {seal_secs:.1}s, {materialized}/{} rows, {blocks} blocks ({empty_blocks} empty)",
		run.name,
		profile_name(profile),
		order.len()
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
