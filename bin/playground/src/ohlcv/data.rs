// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Range;

use reifydb::{
	arrow_array::ArrayRef,
	arrow_schema::FieldRef,
	core::value::column::factory,
	value::value::{
		column_view::ColumnView,
		constraint::{precision::Precision, scale::Scale},
		datetime::DateTime,
		decimal::Decimal,
		value_type::ValueType,
	},
};

use crate::{
	equity,
	solana::{self, SolanaConfig},
	transform::{Derived, Ohlc, forward},
};

pub type Column = (FieldRef, ArrayRef);

pub struct Series {
	pub pool: u32,
	pub key: Vec<i64>,
	pub ohlc: Ohlc,
	pub volume: Vec<u64>,
	pub days: Vec<Range<usize>>,
	pub segments: Vec<Range<usize>>,
	pub derived: Derived,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
	Int8,
	Uint8,
}

pub struct Dataset {
	pub name: &'static str,
	pub key_name: &'static str,
	pub key_type: KeyType,
	pub series: Vec<Series>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Variant {
	Raw,
	Derived,
	Datetime,
	Decimal,
}

impl Variant {
	pub fn label(self) -> &'static str {
		match self {
			Variant::Raw => "raw",
			Variant::Derived => "transformed",
			Variant::Datetime => "raw, ts datetime",
			Variant::Decimal => "raw, decimal(38,2)",
		}
	}
}

pub type RowRef = (usize, usize);

impl Dataset {
	pub fn rows(&self) -> usize {
		self.series.iter().map(|s| s.key.len()).sum()
	}
}

fn split(len: usize, mut boundary: impl FnMut(usize) -> u64) -> Vec<Range<usize>> {
	let mut out = Vec::new();
	let mut start = 0;
	while start < len {
		let day = boundary(start);
		let end = start + (start..len).take_while(|&i| boundary(i) == day).count();
		out.push(start..end);
		start = end;
	}
	out
}

pub fn equity(cfg: &equity::EquityConfig) -> Result<Dataset, String> {
	let bars = equity::generate(cfg);
	let ohlc = Ohlc {
		open: bars.open,
		high: bars.high,
		low: bars.low,
		close: bars.close,
	};
	let len = bars.ts.len();
	let days = split(len, |i| (i / cfg.bars_per_day) as u64);
	let segments = vec![Range {
		start: 0,
		end: len,
	}];
	let derived = forward(&ohlc, &segments)?;
	Ok(Dataset {
		name: "equity",
		key_name: "ts",
		key_type: KeyType::Int8,
		series: vec![Series {
			pool: 0,
			key: bars.ts,
			ohlc,
			volume: bars.volume,
			days,
			segments,
			derived,
		}],
	})
}

pub fn solana(cfg: &SolanaConfig, pools: Vec<solana::Pool>) -> Result<Dataset, String> {
	let day_slots = cfg.day_slots();
	let mut series = Vec::with_capacity(pools.len());
	for pool in pools {
		let bars = pool.bars;
		let ohlc = Ohlc {
			open: bars.open,
			high: bars.high,
			low: bars.low,
			close: bars.close,
		};
		let days = split(bars.slot.len(), |i| (bars.slot[i] - cfg.genesis_slot) / day_slots);
		let derived = forward(&ohlc, &days)?;
		series.push(Series {
			pool: pool.pool_id,
			key: bars.slot.iter().map(|&s| s as i64).collect(),
			ohlc,
			volume: bars.volume,
			segments: days.clone(),
			days,
			derived,
		});
	}
	Ok(Dataset {
		name: "solana",
		key_name: "slot",
		key_type: KeyType::Uint8,
		series,
	})
}

pub fn day_of(cfg: &SolanaConfig, slot: i64) -> u64 {
	(slot as u64 - cfg.genesis_slot) / cfg.day_slots()
}

fn decimal(value: i64) -> Decimal {
	let sign = if value < 0 {
		"-"
	} else {
		""
	};
	let abs = value.unsigned_abs();
	Decimal::parse(&format!("{sign}{}.{:02}", abs / 100, abs % 100)).expect("formatted decimal parses")
}

fn price(name: &str, variant: Variant, values: Vec<i64>) -> Column {
	match variant {
		Variant::Decimal => factory::decimal(
			name,
			Precision::new(38),
			Scale::new(2),
			values.into_iter().map(decimal).collect::<Vec<_>>(),
		),
		_ => factory::int8(name, values),
	}
}

pub fn columns(ds: &Dataset, rows: &[RowRef], variant: Variant, with_pool: bool) -> Vec<Column> {
	let i64s =
		|f: &dyn Fn(&Series, usize) -> i64| rows.iter().map(|&(s, i)| f(&ds.series[s], i)).collect::<Vec<_>>();
	let u64s =
		|f: &dyn Fn(&Series, usize) -> u64| rows.iter().map(|&(s, i)| f(&ds.series[s], i)).collect::<Vec<_>>();
	let key = i64s(&|s, i| s.key[i]);
	let mut out = vec![match (variant, ds.key_type) {
		(Variant::Datetime, _) => factory::datetime(ds.key_name, key.into_iter().map(DateTime::from_nanos)),
		(_, KeyType::Int8) => factory::int8(ds.key_name, key),
		(_, KeyType::Uint8) => factory::uint8(ds.key_name, key.into_iter().map(|k| k as u64)),
	}];
	if with_pool {
		out.push(factory::uint4("pool", rows.iter().map(|&(s, _)| ds.series[s].pool)));
	}
	match variant {
		Variant::Derived => {
			out.push(factory::int8("close", i64s(&|s, i| s.derived.close[i])));
			out.push(factory::int8("gap", i64s(&|s, i| s.derived.gap[i])));
			out.push(factory::uint8("upper", u64s(&|s, i| s.derived.upper[i])));
			out.push(factory::uint8("lower", u64s(&|s, i| s.derived.lower[i])));
		}
		_ => {
			out.push(price("open", variant, i64s(&|s, i| s.ohlc.open[i])));
			out.push(price("high", variant, i64s(&|s, i| s.ohlc.high[i])));
			out.push(price("low", variant, i64s(&|s, i| s.ohlc.low[i])));
			out.push(price("close", variant, i64s(&|s, i| s.ohlc.close[i])));
		}
	}
	out.push(factory::uint8("volume", u64s(&|s, i| s.volume[i])));
	out
}

pub fn value_type(column: &Column) -> ValueType {
	ColumnView::try_from(column).expect("factory columns have a view").get_type()
}
