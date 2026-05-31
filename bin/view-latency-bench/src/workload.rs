// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Workload {
	Synthetic,
	Pump,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Kind {
	Transactional,
	Deferred,
}

impl Kind {
	pub fn create(self) -> &'static str {
		match self {
			Kind::Transactional => "create view",
			Kind::Deferred => "create deferred view",
		}
	}

	pub fn label(self) -> &'static str {
		match self {
			Kind::Transactional => "transactional",
			Kind::Deferred => "deferred",
		}
	}
}

pub fn schema(workload: Workload) -> Vec<String> {
	match workload {
		Workload::Synthetic => {
			vec!["create table bench::source { id: int8, sym: utf8, price: float8, volume: float8 }"
				.to_string()]
		}
		Workload::Pump => vec![
			"create table bench::source { id: int8, slot: uint8, timestamp: datetime, base_mint: utf8, \
			 quote_mint: utf8, vwap: float8, total_volume: float8, vwap_usd: float8, volume_usd: float8 }"
				.to_string(),
		],
	}
}

pub fn probe_view(kind: Kind) -> String {
	format!("{create} bench::probe {{ id: int8 }} as {{ from bench::source map {{ id }} }}", create = kind.create())
}

pub fn workload_view(workload: Workload, kind: Kind, i: usize) -> String {
	match workload {
		Workload::Synthetic => format!(
			"{create} bench::w{i} {{ id: int8, sym: utf8, price: float8, adj: float8 }} as {{ from \
			 bench::source filter {{ id >= 0 }} map {{ id, sym, price, adj: price + cast({i}, float8) }} }}",
			create = kind.create()
		),
		Workload::Pump => format!(
			"{create} bench::w{i} {{ base_mint: utf8, quote_mint: utf8, timestamp: datetime, slot: uint8, \
			 window_duration: Duration, vwap: float8, vwap_usd: float8, current_price: float8, \
			 current_price_usd: float8, cumulative_price_volume: float8, cumulative_price_volume_usd: float8, \
			 total_volume: float8, total_volume_usd: float8, data_points: uint4, window_start: datetime, \
			 window_end: datetime }} as {{ from bench::source apply vwap {{ window_duration: {dur}, \
			 emit_on_close: false, price: \"vwap\", volume: \"total_volume\", price_usd: \"vwap_usd\", \
			 volume_usd: \"volume_usd\" }} }}",
			create = kind.create(),
			dur = 1 + (i % 5)
		),
	}
}

pub fn insert_row(workload: Workload, id: i64) -> String {
	match workload {
		Workload::Synthetic => {
			let price = (id % 997) as f64 + 1.0;
			format!("INSERT bench::source [{{ id: {id}, sym: \"S\", price: {price:.1}, volume: 1.0 }}]")
		}
		Workload::Pump => {
			let vwap = (id % 997) as f64 + 1.0;
			let s = id.rem_euclid(86_400);
			let (hh, mm, ss) = (s / 3600, (s % 3600) / 60, s % 60);
			format!("INSERT bench::source [{{ id: {id}, slot: {id}, timestamp: \
				 2024-01-01T{hh:02}:{mm:02}:{ss:02}Z, base_mint: \"B\", quote_mint: \"Q\", vwap: \
				 {vwap:.4}, total_volume: 10.0, vwap_usd: {vwap:.4}, volume_usd: 100.0 }}]")
		}
	}
}

pub fn probe_query(id: i64) -> String {
	format!("from bench::probe filter {{ id == {id} }}")
}
