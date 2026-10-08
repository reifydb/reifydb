// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb::{
	Clock, Error,
	arrow_array::{
		ArrayRef,
		cast::AsArray,
		types::{Int64Type, UInt64Type},
	},
	column::{
		compress::{CompressionProfile, Compressor},
		convert::to_arrow,
		device::BlockKey,
		persist::serialize_block,
		session::new_session,
		snapshot::ColumnBlock,
		store::ColumnStore,
	},
};

use crate::{
	data::{Column, Dataset, RowRef, Variant, columns, day_of, value_type},
	profile_name,
	report::{ColumnTally, Row},
	solana::SolanaConfig,
	transform::{Derived, inverse},
};

pub struct Layout {
	pub name: &'static str,
	pub blocks: Vec<Vec<RowRef>>,
	pub with_pool: bool,
}

pub fn equity_layouts(ds: &Dataset) -> (Layout, Layout) {
	let series = &ds.series[0];
	let one = Layout {
		name: "1 block",
		blocks: vec![(0..series.key.len()).map(|i| (0, i)).collect()],
		with_pool: false,
	};
	let daily = Layout {
		name: "390-row blocks",
		blocks: series.days.iter().map(|day| day.clone().map(|i| (0, i)).collect()).collect(),
		with_pool: false,
	};
	(one, daily)
}

pub fn solana_layouts(ds: &Dataset, cfg: &SolanaConfig) -> Vec<Layout> {
	let per_pool = ds
		.series
		.iter()
		.enumerate()
		.flat_map(|(s, series)| series.days.iter().map(move |day| day.clone().map(|i| (s, i)).collect()))
		.collect();
	let mut by_day: BTreeMap<u64, Vec<RowRef>> = BTreeMap::new();
	for (s, series) in ds.series.iter().enumerate() {
		for (i, &key) in series.key.iter().enumerate() {
			by_day.entry(day_of(cfg, key)).or_default().push((s, i));
		}
	}
	let clustered: Vec<Vec<RowRef>> = by_day.into_values().collect();
	let interleaved = clustered
		.iter()
		.map(|rows| {
			let mut rows = rows.clone();
			rows.sort_by_key(|&(s, i)| (ds.series[s].key[i], ds.series[s].pool));
			rows
		})
		.collect();
	vec![
		Layout {
			name: "L1 pool x day",
			blocks: per_pool,
			with_pool: false,
		},
		Layout {
			name: "L2 day, by slot",
			blocks: interleaved,
			with_pool: true,
		},
		Layout {
			name: "L3 day, by pool",
			blocks: clustered,
			with_pool: true,
		},
	]
}

fn i64s(array: &ArrayRef) -> Vec<i64> {
	array.as_primitive::<Int64Type>().values().to_vec()
}

fn u64s(array: &ArrayRef) -> Vec<u64> {
	array.as_primitive::<UInt64Type>().values().to_vec()
}

fn by_name<'a>(names: &[String], arrays: &'a [ArrayRef], name: &str) -> &'a ArrayRef {
	&arrays[names.iter().position(|n| n == name).expect("column present")]
}

fn verify_transform(ds: &Dataset, layout: &Layout, restored: &[Vec<ArrayRef>], names: &[String]) -> Result<(), String> {
	let mut derived: Vec<Derived> = ds
		.series
		.iter()
		.map(|s| {
			let n = s.key.len();
			Derived {
				close: vec![0; n],
				gap: vec![0; n],
				upper: vec![0; n],
				lower: vec![0; n],
			}
		})
		.collect();
	let mut seen: Vec<Vec<bool>> = ds.series.iter().map(|s| vec![false; s.key.len()]).collect();
	for (rows, arrays) in layout.blocks.iter().zip(restored) {
		let close = i64s(by_name(names, arrays, "close"));
		let gap = i64s(by_name(names, arrays, "gap"));
		let upper = u64s(by_name(names, arrays, "upper"));
		let lower = u64s(by_name(names, arrays, "lower"));
		for (j, &(s, i)) in rows.iter().enumerate() {
			if seen[s][i] {
				return Err(format!("row {s}/{i} stored twice"));
			}
			seen[s][i] = true;
			derived[s].close[i] = close[j];
			derived[s].gap[i] = gap[j];
			derived[s].upper[i] = upper[j];
			derived[s].lower[i] = lower[j];
		}
	}
	for (s, series) in ds.series.iter().enumerate() {
		if let Some(i) = seen[s].iter().position(|&v| !v) {
			return Err(format!("row {s}/{i} missing"));
		}
		let ohlc = inverse(&derived[s], &series.segments)?;
		if ohlc != series.ohlc {
			return Err(format!("inverse transform differs for series {}", series.pool));
		}
	}
	Ok(())
}

pub fn measure(ds: &Dataset, layout: &Layout, variant: Variant, profile: CompressionProfile) -> Result<Row, String> {
	let err = |e: Error| e.to_string();
	let session = new_session();
	let compressor = Compressor::with_profile(session.clone(), profile);
	let store = ColumnStore::memory().map_err(err)?;
	let mut tally = ColumnTally::default();
	let mut bytes = 0u64;
	let mut inputs: Vec<Vec<Column>> = Vec::with_capacity(layout.blocks.len());
	let mut names: Vec<String> = Vec::new();
	for (b, rows) in layout.blocks.iter().enumerate() {
		let cols = columns(ds, rows, variant, layout.with_pool);
		names = cols.iter().map(|c| c.0.name().to_string()).collect();
		let mut schema = Vec::with_capacity(cols.len());
		let mut chunks = Vec::with_capacity(cols.len());
		for col in &cols {
			let ty = value_type(col);
			let chunk = compressor.compress(ty.clone(), col).map_err(err)?;
			let single = ColumnBlock::new(
				Arc::new(vec![(col.0.name().to_string(), ty.clone(), false)]),
				vec![chunk.clone()],
			);
			let single_bytes = serialize_block(&single, &session).map_err(err)?.len() as u64;
			tally.add(col.0.name(), single_bytes, chunk.chunks[0].encoding_id().to_string());
			schema.push((col.0.name().to_string(), ty, false));
			chunks.push(chunk);
		}
		let block = ColumnBlock::new(Arc::new(schema), chunks);
		bytes += serialize_block(&block, &session).map_err(err)?.len() as u64;
		store.write(
			&BlockKey {
				dir: 1,
				name: b as u64,
			},
			&block,
		)
		.map_err(err)?;
		inputs.push(cols);
	}

	let handles = (0..layout.blocks.len())
		.map(|b| {
			store.open(&BlockKey {
				dir: 1,
				name: b as u64,
			})
			.map_err(err)?
			.ok_or_else(|| format!("block {b} missing"))
		})
		.collect::<Result<Vec<_>, String>>()?;

	let mut restored: Vec<Vec<ArrayRef>> = Vec::with_capacity(handles.len());
	let mut verified: Result<(), String> = Ok(());
	for (b, handle) in handles.iter().enumerate() {
		let block = handle.read(None).map_err(err)?;
		let mut arrays = Vec::with_capacity(block.columns.len());
		for ((name, _, _), column) in block.schema.iter().zip(&block.columns) {
			if column.chunks.len() != 1 {
				verified = Err(format!("block {b} column {name} has {} chunks", column.chunks.len()));
			}
			let (_, array) =
				to_arrow(&session, name, &column.field_type, column.chunks[0].clone()).map_err(err)?;
			arrays.push(array);
		}
		for (input, array) in inputs[b].iter().zip(&arrays) {
			if verified.is_ok() && input.1.as_ref() != array.as_ref() {
				verified = Err(format!("block {b} column {} differs after read-back", input.0.name()));
			}
		}
		restored.push(arrays);
	}
	if verified.is_ok() && variant == Variant::Derived {
		verified = verify_transform(ds, layout, &restored, &names);
	}
	drop(restored);
	drop(inputs);

	let clock = Clock::Real;
	let mut best = f64::MAX;
	for _ in 0..5 {
		let start = clock.instant();
		for handle in &handles {
			let block = handle.read(None).map_err(err)?;
			for ((name, _, _), column) in block.schema.iter().zip(&block.columns) {
				for chunk in &column.chunks {
					to_arrow(&session, name, &column.field_type, chunk.clone()).map_err(err)?;
				}
			}
		}
		best = best.min(start.elapsed().as_secs_f64());
	}

	let rows: usize = layout.blocks.iter().map(Vec::len).sum();
	Ok(Row {
		phase: "1",
		dataset: ds.name.to_string(),
		layout: layout.name.to_string(),
		columns: variant.label().to_string(),
		profile: profile_name(profile).to_string(),
		blocks: layout.blocks.len(),
		rows,
		bytes,
		system_bytes: None,
		decode_mbps: Some(rows as f64 * 48.0 / best / 1e6),
		verified: match verified {
			Ok(()) => "yes".to_string(),
			Err(e) => format!("NO: {e}"),
		},
		per_column: tally.finish(&names),
	})
}
