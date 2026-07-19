// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_profiler::{
	category::ProfilerCategory,
	intern::DimInterner,
	percentile::PercentileHistogram,
	record::{AggregateRecord, MAX_EXTRAS, SpanIdent},
};

use super::instruments::ProfilerInstruments;

pub struct ProfilerAccumulator {
	records: HashMap<SpanIdent, AggregateRecord>,
	capacity: usize,
	min_calls_for_retention: u64,
	instruments: Arc<ProfilerInstruments>,
}

impl ProfilerAccumulator {
	pub fn new(capacity: usize, min_calls_for_retention: u64, instruments: Arc<ProfilerInstruments>) -> Self {
		instruments.accumulator_capacity.set(capacity as f64);
		Self {
			records: HashMap::with_capacity(capacity.min(4096)),
			capacity,
			min_calls_for_retention,
			instruments,
		}
	}

	pub fn upsert(
		&mut self,
		ident: SpanIdent,
		span_name: &'static str,
		duration_us: u32,
		extras: &[u64; MAX_EXTRAS],
		interner: &DimInterner,
	) {
		if let Some(existing) = self.records.get_mut(&ident) {
			existing.fold(duration_us, extras);
			return;
		}
		if self.records.len() >= self.capacity {
			self.evict_lfu();
		}
		let mut new_record = AggregateRecord {
			category: ident.category,
			span_name: span_name.to_string(),
			dimensions: resolve_dims(&ident, interner),
			calls: 0,
			total_us: 0,
			histogram: PercentileHistogram::new(),
			extras_sum: [0; MAX_EXTRAS],
		};
		new_record.fold(duration_us, extras);
		self.records.insert(ident, new_record);
		self.instruments.accumulator_size.set(self.records.len() as f64);
	}

	pub fn top_n(&self, category: ProfilerCategory, n: usize) -> Vec<AggregateRecord> {
		let mut filtered: Vec<&AggregateRecord> =
			self.records.values().filter(|r| r.category == category).collect();
		filtered.sort_by(|a, b| b.total_us.cmp(&a.total_us));
		filtered.into_iter().take(n).cloned().collect()
	}

	pub fn all(&self) -> Vec<AggregateRecord> {
		self.records.values().cloned().collect()
	}

	pub fn len(&self) -> usize {
		self.records.len()
	}

	pub fn is_empty(&self) -> bool {
		self.records.is_empty()
	}

	fn evict_lfu(&mut self) {
		let mut victim: Option<(SpanIdent, u64)> = None;
		for (ident, record) in &self.records {
			let calls = record.calls;
			if calls >= self.min_calls_for_retention && self.min_calls_for_retention > 0 {
				continue;
			}
			match victim {
				None => victim = Some((*ident, calls)),
				Some((_, v_calls)) if calls < v_calls => victim = Some((*ident, calls)),
				_ => {}
			}
		}
		if let Some((ident, _)) = victim {
			self.records.remove(&ident);
			self.instruments.accumulator_evictions.inc();
		} else if let Some((ident, _)) =
			self.records.iter().min_by_key(|(_, r)| r.calls).map(|(k, v)| (*k, v.calls))
		{
			self.records.remove(&ident);
			self.instruments.accumulator_evictions.inc();
		}
	}
}

fn resolve_dims(ident: &SpanIdent, interner: &DimInterner) -> Vec<String> {
	let mut dims = Vec::new();
	for idx in ident.dim_indices {
		if let Some(s) = interner.resolve(idx) {
			dims.push(s);
		} else {
			dims.push(String::new());
		}
	}
	while dims.last().map(|s| s.is_empty()).unwrap_or(false) {
		dims.pop();
	}
	dims
}

#[cfg(test)]
mod tests {
	use reifydb_profiler::record::DIM_UNSET;

	use super::*;

	fn make_ident(category: ProfilerCategory, callsite: u64) -> SpanIdent {
		SpanIdent::new(category, callsite, [DIM_UNSET; 2])
	}

	#[test]
	fn upsert_folds_repeats() {
		let interner = DimInterner::new();
		let mut acc = ProfilerAccumulator::new(8, 0, Arc::new(ProfilerInstruments::new()));
		let ident = make_ident(ProfilerCategory::Query, 1);
		acc.upsert(ident, "vm::execute", 100, &[0; MAX_EXTRAS], &interner);
		acc.upsert(ident, "vm::execute", 50, &[0; MAX_EXTRAS], &interner);
		acc.upsert(ident, "vm::execute", 200, &[0; MAX_EXTRAS], &interner);
		assert_eq!(acc.len(), 1);
		let rec = acc.records.get(&ident).unwrap();
		assert_eq!(rec.calls, 3);
		assert_eq!(rec.total_us, 350);
		assert_eq!(rec.histogram.total_count(), 3);
	}

	#[test]
	fn capacity_cap_triggers_eviction() {
		let interner = DimInterner::new();
		let mut acc = ProfilerAccumulator::new(2, 0, Arc::new(ProfilerInstruments::new()));
		for i in 1..=3 {
			let ident = make_ident(ProfilerCategory::Storage, i);
			acc.upsert(ident, "store::single::get", 10, &[0; MAX_EXTRAS], &interner);
		}
		assert!(acc.len() <= 2);
	}

	#[test]
	fn top_n_orders_by_total_us() {
		let interner = DimInterner::new();
		let mut acc = ProfilerAccumulator::new(8, 0, Arc::new(ProfilerInstruments::new()));
		let a = make_ident(ProfilerCategory::Flow, 1);
		let b = make_ident(ProfilerCategory::Flow, 2);
		acc.upsert(a, "flow::engine::apply", 100, &[0; MAX_EXTRAS], &interner);
		acc.upsert(a, "flow::engine::apply", 100, &[0; MAX_EXTRAS], &interner);
		acc.upsert(b, "flow::engine::apply", 500, &[0; MAX_EXTRAS], &interner);

		let top = acc.top_n(ProfilerCategory::Flow, 5);
		assert_eq!(top.len(), 2);
		assert_eq!(top[0].total_us, 500);
		assert_eq!(top[1].total_us, 200);
	}
}
