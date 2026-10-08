// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	ops::Range,
	sync::atomic::{AtomicBool, AtomicUsize, Ordering},
	thread,
};

use reifydb::column::compress::CompressionProfile;

use crate::{
	data::{Dataset, KeyType, Series, Variant},
	phase1::{Layout, measure},
	phase2, profile_name, profiles,
	report::{self, ColumnTally, Row},
	solana::Pool,
	solana1s::{Class, DAY_SECONDS, Sample, SecondConfig, asset, samples},
	transform::{Ohlc, forward},
};

const VARIANTS: [Variant; 2] = [Variant::Raw, Variant::Derived];
const THREADS: usize = 16;

struct Measured {
	width_days: u64,
	rownum: bool,
	variant: Variant,
	profile: CompressionProfile,
	row: Row,
}

fn band(sample: &Sample) -> &'static str {
	match (sample.class, sample.rank) {
		(Class::Infrequent, _) => "infrequent",
		(Class::Frequent, 1..=10) => "frequent 1-10",
		(Class::Frequent, 11..=100) => "frequent 11-100",
		(Class::Frequent, 101..=1000) => "frequent 101-1k",
		(Class::Frequent, _) => "frequent 1k-10k",
	}
}

fn shape(rownum: bool) -> &'static str {
	if rownum {
		" +#rownum"
	} else {
		""
	}
}

fn class_name(class: Class) -> &'static str {
	match class {
		Class::Frequent => "frequent",
		Class::Infrequent => "infrequent",
	}
}

fn buckets(keys: &[i64], width: u64) -> Vec<Range<usize>> {
	let mut out: Vec<Range<usize>> = Vec::new();
	for (i, &key) in keys.iter().enumerate() {
		let bucket = key as u64 / width;
		match out.last_mut() {
			Some(last) if keys[last.start] as u64 / width == bucket => last.end = i + 1,
			_ => out.push(i..i + 1),
		}
	}
	out
}

fn dataset(pool: &Pool, end: u64, width: u64) -> Result<Dataset, String> {
	let bars = &pool.bars;
	let n = bars.slot.partition_point(|&s| s < end);
	let key: Vec<i64> = bars.slot[..n].iter().map(|&s| s as i64).collect();
	let ohlc = Ohlc {
		open: bars.open[..n].to_vec(),
		high: bars.high[..n].to_vec(),
		low: bars.low[..n].to_vec(),
		close: bars.close[..n].to_vec(),
	};
	let segments = buckets(&key, width);
	let derived = forward(&ohlc, &segments)?;
	Ok(Dataset {
		name: "solana1s",
		key_name: "ts",
		key_type: KeyType::Uint8,
		series: vec![Series {
			pool: pool.pool_id,
			key,
			ohlc,
			volume: bars.volume[..n].to_vec(),
			days: segments.clone(),
			segments,
			derived,
		}],
	})
}

fn measure_asset(cfg: &SecondConfig, sample: &Sample) -> Result<Vec<Measured>, String> {
	let pool = asset(cfg, sample);
	let mut out = Vec::new();
	for width_days in 1..=7u64 {
		let full = cfg.days as u64 / width_days;
		if full == 0 {
			continue;
		}
		let width = width_days * DAY_SECONDS;
		let ds = dataset(&pool, cfg.start + full * width, width)?;
		let blocks: Vec<Vec<_>> =
			ds.series[0].segments.iter().map(|r| r.clone().map(|i| (0, i)).collect()).collect();
		for rownum in [false, true] {
			let layout = Layout {
				name: "per asset",
				blocks: blocks.clone(),
				with_pool: false,
				with_rownum: rownum,
			};
			for variant in VARIANTS {
				for profile in profiles() {
					let row = measure(&ds, &layout, variant, profile)?;
					out.push(Measured {
						width_days,
						rownum,
						variant,
						profile,
						row,
					});
				}
			}
		}
	}
	Ok(out)
}

#[derive(Default)]
struct Aggregate {
	assets: usize,
	weight: f64,
	rows: f64,
	bytes: f64,
	blocks: f64,
	failures: Vec<String>,
	tally: ColumnTally,
	names: Vec<String>,
}

impl Aggregate {
	fn add(&mut self, sample: &Sample, row: &Row) {
		self.assets += 1;
		self.weight += sample.weight;
		self.rows += sample.weight * row.rows as f64;
		self.bytes += sample.weight * row.bytes as f64;
		self.blocks += sample.weight * row.blocks as f64;
		if row.verified != "yes" {
			self.failures.push(format!(
				"{} rank {}: {}",
				class_name(sample.class),
				sample.rank,
				row.verified
			));
		}
		if self.names.is_empty() {
			self.names = row.per_column.iter().map(|c| c.name.clone()).collect();
		}
		for column in &row.per_column {
			self.tally.merge(
				&column.name,
				(sample.weight * column.bytes as f64).round() as u64,
				&column.encodings,
			);
		}
	}

	fn into_row(self, group: &str, key: &Key) -> Row {
		Row {
			phase: "1",
			dataset: "solana1s".to_string(),
			layout: format!("{group}, {}h", key.0 * 24),
			columns: format!("{}{}", key.2, shape(key.1)),
			profile: key.3.to_string(),
			blocks: self.blocks.round() as usize,
			rows: self.rows.round() as usize,
			bytes: self.bytes.round() as u64,
			system_bytes: None,
			decode_mbps: None,
			verified: if self.failures.is_empty() {
				format!("yes ({} assets)", self.assets)
			} else {
				format!("NO: {} of {}: {}", self.failures.len(), self.assets, self.failures[0])
			},
			per_column: self.tally.finish(&self.names),
		}
	}
}

type Key = (u64, bool, &'static str, &'static str);

fn run_samples(cfg: &SecondConfig, picked: &[Sample]) -> Result<Vec<(Sample, Vec<Measured>)>, String> {
	let next = AtomicUsize::new(0);
	let done = AtomicUsize::new(0);
	let failed = AtomicBool::new(false);
	thread::scope(|scope| {
		let workers: Vec<_> = (0..THREADS)
			.map(|_| {
				scope.spawn(|| -> Result<Vec<(Sample, Vec<Measured>)>, String> {
					let mut out = Vec::new();
					loop {
						let i = next.fetch_add(1, Ordering::Relaxed);
						if i >= picked.len() || failed.load(Ordering::Relaxed) {
							return Ok(out);
						}
						let measured = measure_asset(cfg, &picked[i]).map_err(|e| {
							failed.store(true, Ordering::Relaxed);
							format!("{:?} rank {}: {e}", picked[i].class, picked[i].rank)
						})?;
						out.push((picked[i], measured));
						let finished = done.fetch_add(1, Ordering::Relaxed) + 1;
						if finished % 250 == 0 {
							eprintln!("measured {finished} of {} assets", picked.len());
						}
					}
				})
			})
			.collect();
		let mut all = Vec::with_capacity(picked.len());
		for worker in workers {
			all.extend(worker.join().map_err(|_| "measure thread panicked".to_string())??);
		}
		Ok(all)
	})
}

fn per_day(row: &Row, width_days: u64, cfg: &SecondConfig) -> f64 {
	row.bytes as f64 / ((cfg.days as u64 / width_days) * width_days) as f64
}

fn print_totals(cfg: &SecondConfig, totals: &BTreeMap<Key, (Row, Row)>) {
	println!(
		"\nextrapolated to {} frequent + {} infrequent assets (bytes per asset per day, total per day and per week)",
		cfg.frequent, cfg.infrequent
	);
	println!(
		"{:>5} {:<22} {:<8} {:>8} {:>12} {:>8} {:>12} {:>13} {:>9} {:>10}",
		"width",
		"columns",
		"profile",
		"freq B/r",
		"freq B/a/d",
		"infr B/r",
		"infr B/a/d",
		"rows/day",
		"GB/day",
		"GB/week"
	);
	for (key, (frequent, infrequent)) in totals {
		let b_per_row = |row: &Row| row.bytes as f64 / row.rows.max(1) as f64;
		let f_day = per_day(frequent, key.0, cfg);
		let i_day = per_day(infrequent, key.0, cfg);
		let used = ((cfg.days as u64 / key.0) * key.0) as f64;
		let rows_day = (frequent.rows + infrequent.rows) as f64 / used;
		println!(
			"{:>4}h {:<22} {:<8} {:>8.2} {:>12.0} {:>8.2} {:>12.0} {:>13.0} {:>9.2} {:>10.2}",
			key.0 * 24,
			format!("{}{}", key.2, shape(key.1)),
			key.3,
			b_per_row(frequent),
			f_day / cfg.frequent as f64,
			b_per_row(infrequent),
			i_day / cfg.infrequent as f64,
			rows_day,
			(f_day + i_day) / 1e9,
			(f_day + i_day) * 7.0 / 1e9
		);
	}
	println!("\ncost of #rownum (B/row and GB/day over all rows, shape with #rownum minus without)");
	println!(
		"{:>5} {:<12} {:<8} {:>9} {:>9} {:>9} {:>9}",
		"width", "columns", "profile", "freq B/r", "infr B/r", "all B/r", "GB/day"
	);
	for (key, (frequent, infrequent)) in totals.iter().filter(|(key, _)| !key.1) {
		let Some((frequent_b, infrequent_b)) = totals.get(&(key.0, true, key.2, key.3)) else {
			continue;
		};
		let used = ((cfg.days as u64 / key.0) * key.0) as f64;
		let delta = |a: &Row, b: &Row| b.bytes as f64 - a.bytes as f64;
		let (f, i) = (delta(frequent, frequent_b), delta(infrequent, infrequent_b));
		println!(
			"{:>4}h {:<12} {:<8} {:>9.3} {:>9.3} {:>9.3} {:>9.3}",
			key.0 * 24,
			key.2,
			key.3,
			f / frequent.rows.max(1) as f64,
			i / infrequent.rows.max(1) as f64,
			(f + i) / (frequent.rows + infrequent.rows).max(1) as f64,
			(f + i) / used / 1e9
		);
	}
}

pub fn run(cfg: &SecondConfig) -> Result<(), String> {
	let picked = samples(cfg);
	println!(
		"solana1s: {} frequent and {} infrequent assets sampled, {} days of 1 s bars from unix {}",
		picked.iter().filter(|s| s.class == Class::Frequent).count(),
		picked.iter().filter(|s| s.class == Class::Infrequent).count(),
		cfg.days,
		cfg.start
	);
	let measured = run_samples(cfg, &picked)?;

	let mut bands: BTreeMap<(&'static str, Key), Aggregate> = BTreeMap::new();
	let mut classes: BTreeMap<(Class, Key), Aggregate> = BTreeMap::new();
	for (sample, rows) in &measured {
		for m in rows {
			let key = (m.width_days, m.rownum, m.variant.label(), profile_name(m.profile));
			bands.entry((band(sample), key)).or_default().add(sample, &m.row);
			classes.entry((sample.class, key)).or_default().add(sample, &m.row);
		}
	}

	let mut band_rows: Vec<(Key, Row)> =
		bands.into_iter().map(|((group, key), agg)| (key, agg.into_row(group, &key))).collect();
	band_rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.layout.cmp(&b.1.layout)));
	let band_rows: Vec<Row> = band_rows.into_iter().map(|(_, row)| row).collect();
	report::print("solana1s: per asset blocks, weighted to the full population", &band_rows);

	let mut totals: BTreeMap<Key, (Option<Row>, Option<Row>)> = BTreeMap::new();
	for ((class, key), agg) in classes {
		let row = agg.into_row(class_name(class), &key);
		let entry = totals.entry(key).or_default();
		match class {
			Class::Frequent => entry.0 = Some(row),
			Class::Infrequent => entry.1 = Some(row),
		}
	}
	let totals: BTreeMap<Key, (Row, Row)> =
		totals.into_iter().filter_map(|(key, (f, i))| Some((key, (f?, i?)))).collect();
	print_totals(cfg, &totals);

	let csv = phase2::root().join("results-1s-rownum.csv");
	let class_rows: Vec<Row> = totals.into_values().flat_map(|(f, i)| [f, i]).collect();
	report::write_csv(&csv, &band_rows.into_iter().chain(class_rows).collect::<Vec<_>>())?;
	println!("\nwrote {}", csv.display());
	Ok(())
}
