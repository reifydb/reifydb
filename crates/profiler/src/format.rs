// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Human-readable summary formatters. Two entry points:
//!
//! - `summary` produces the byte-identical one-line output that chaindex's `commit-profiler` used. The line is the
//!   public contract for chaindex's bulk-commit log scraping and stays stable.
//! - `summary_table` produces a multi-line aligned table for ad-hoc human inspection - not for log lines.
//!
//! Both resolve operator dimension labels (`node_type`/`node_id` for Flow records) via the optional `DimInterner`
//! carried on `ProfileSummary`. When the interner is absent or the index is uninterned, labels fall back to `?`
//! plus the dimension index for visibility.

use std::{cmp::Reverse, collections::HashMap, fmt::Write};

use crate::{
	category::{ALL_CATEGORIES, ProfileCategory},
	intern::DimInterner,
	record::{AggregateRecord, DimIdx},
	summary::ProfileSummary,
};

pub fn summary(summary: &ProfileSummary, top_n: usize) -> String {
	let (totals, mut hot) = flow_aggregates(summary);

	let mut out = format!(
		"tick(proc={}us/{}c, apply_sum={}us/{}ops, lock_sum={}us)",
		totals.process_wall_us,
		totals.process_calls,
		totals.apply_total_us,
		totals.op_calls,
		totals.lock_total_us,
	);

	if hot.is_empty() {
		out.push_str(" hot=[]");
		return out;
	}

	hot.sort_by(|a, b| b.1.apply_us.cmp(&a.1.apply_us));
	out.push_str(" hot=[");
	for (i, (key, entry)) in hot.iter().take(top_n).enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		let label = resolve_label(summary.interner.as_deref(), key.0);
		let id = resolve_id(summary.interner.as_deref(), key.1);
		let _ = write!(
			out,
			"{}@{}={}us/{}lk/{}c/{}in/{}out",
			label, id, entry.apply_us, entry.lock_us, entry.calls, entry.input_rows, entry.output_rows,
		);
	}
	out.push(']');
	out
}

pub fn summary_table(summary: &ProfileSummary, top_n: usize) -> String {
	let mut out = String::new();
	let _ = writeln!(out, "profile scope={} total={}", summary.scope_name, fmt_us(summary.total_duration_us));

	for cat in ALL_CATEGORIES {
		let cat_summary = summary.category(cat);
		if cat_summary.calls == 0 {
			continue;
		}

		match cat {
			ProfileCategory::Flow => {
				let _ = writeln!(
					out,
					"  {}: {} calls, apply={}, lock={}",
					category_label(cat),
					cat_summary.calls,
					fmt_us(cat_summary.total_us),
					fmt_us(cat_summary.extras_sum[2]),
				);
				render_flow_rows(&mut out, summary, top_n);
			}
			_ => {
				let _ = writeln!(
					out,
					"  {}: {} calls, total={}",
					category_label(cat),
					cat_summary.calls,
					fmt_us(cat_summary.total_us),
				);
				render_non_flow_rows(&mut out, summary, cat, top_n);
			}
		}
	}

	out
}

pub fn aggregates_table(records: &[AggregateRecord], top_n: usize) -> String {
	let mut out = String::new();
	if records.is_empty() {
		out.push_str("profile (accumulator) empty\n");
		return out;
	}
	let total_calls: u64 = records.iter().map(|r| r.calls).sum();
	let total_us: u64 = records.iter().map(|r| r.total_us).sum();
	let _ = writeln!(
		out,
		"profile (accumulator) {} records, {} calls, total={}",
		records.len(),
		total_calls,
		fmt_us(total_us)
	);

	for cat in ALL_CATEGORIES {
		let cat_records: Vec<&AggregateRecord> = records.iter().filter(|r| r.category == cat).collect();
		if cat_records.is_empty() {
			continue;
		}
		let cat_calls: u64 = cat_records.iter().map(|r| r.calls).sum();
		let cat_total: u64 = cat_records.iter().map(|r| r.total_us).sum();
		let _ = writeln!(
			out,
			"  {}: {} records, {} calls, total={}",
			category_label(cat),
			cat_records.len(),
			cat_calls,
			fmt_us(cat_total)
		);

		let mut by_name: HashMap<&str, Vec<&AggregateRecord>> = HashMap::new();
		for r in &cat_records {
			by_name.entry(r.span_name.as_str()).or_default().push(*r);
		}
		let mut groups: Vec<(&str, Vec<&AggregateRecord>)> = by_name.into_iter().collect();
		groups.sort_by_key(|(_, recs)| Reverse(recs.iter().map(|r| r.total_us).sum::<u64>()));

		for (span_name, mut group) in groups {
			let group_total: u64 = group.iter().map(|r| r.total_us).sum();
			let group_calls: u64 = group.iter().map(|r| r.calls).sum();

			if group.len() == 1 && group[0].dimensions.is_empty() {
				let r = group[0];
				let p = r.histogram.percentiles();
				let _ = writeln!(
					out,
					"    {}  total={} calls={} p50={} p75={} p90={} p95={} p99={}",
					span_name,
					fmt_us(r.total_us),
					r.calls,
					fmt_us(p.p50 as u64),
					fmt_us(p.p75 as u64),
					fmt_us(p.p90 as u64),
					fmt_us(p.p95 as u64),
					fmt_us(p.p99 as u64),
				);
				continue;
			}

			let _ = writeln!(
				out,
				"    {} [{} ops, total={}, calls={}]",
				span_name,
				group.len(),
				fmt_us(group_total),
				group_calls,
			);

			group.sort_by(|a, b| b.total_us.cmp(&a.total_us));
			group.truncate(top_n);

			let labels: Vec<String> = group
				.iter()
				.map(|r| {
					if r.dimensions.is_empty() {
						"<no-dims>".to_string()
					} else {
						r.dimensions.join("@")
					}
				})
				.collect();
			let max_label_width = labels.iter().map(|s| s.len()).max().unwrap_or(0);

			for (i, r) in group.iter().enumerate() {
				let p = r.histogram.percentiles();
				let _ = writeln!(
					out,
					"      {:<width$}  total={} calls={} p50={} p75={} p90={} p95={} p99={}",
					labels[i],
					fmt_us(r.total_us),
					r.calls,
					fmt_us(p.p50 as u64),
					fmt_us(p.p75 as u64),
					fmt_us(p.p90 as u64),
					fmt_us(p.p95 as u64),
					fmt_us(p.p99 as u64),
					width = max_label_width,
				);
			}
		}
	}

	out
}

pub fn fmt_us(us: u64) -> String {
	if us < 1_000 {
		format!("{}us", us)
	} else if us < 1_000_000 {
		format!("{:.1}ms", us as f64 / 1_000.0)
	} else {
		format!("{:.1}s", us as f64 / 1_000_000.0)
	}
}

#[derive(Default)]
struct FlowTotals {
	apply_total_us: u64,
	op_calls: u32,
	process_wall_us: u64,
	process_calls: u32,
	lock_total_us: u64,
}

#[derive(Default, Clone)]
struct HotEntry {
	apply_us: u64,
	lock_us: u64,
	calls: u32,
	input_rows: u64,
	output_rows: u64,
}

type FlowKey = (DimIdx, DimIdx);

fn flow_aggregates(summary: &ProfileSummary) -> (FlowTotals, Vec<(FlowKey, HotEntry)>) {
	let mut totals = FlowTotals::default();
	let mut aggregates: HashMap<FlowKey, HotEntry> = HashMap::new();

	for r in &summary.records {
		if r.category_id != ProfileCategory::Flow as u8 {
			continue;
		}
		let is_apply = r.dim_indices[0] != 0 || r.dim_indices[1] != 0;
		if is_apply {
			totals.apply_total_us = totals.apply_total_us.saturating_add(r.duration_us as u64);
			totals.op_calls = totals.op_calls.saturating_add(1);
			totals.lock_total_us = totals.lock_total_us.saturating_add(r.extras[2]);
			let entry = aggregates.entry((r.dim_indices[0], r.dim_indices[1])).or_default();
			entry.apply_us = entry.apply_us.saturating_add(r.duration_us as u64);
			entry.lock_us = entry.lock_us.saturating_add(r.extras[2]);
			entry.calls = entry.calls.saturating_add(1);
			entry.input_rows = entry.input_rows.saturating_add(r.extras[0]);
			entry.output_rows = entry.output_rows.saturating_add(r.extras[1]);
		} else {
			totals.process_wall_us = totals.process_wall_us.saturating_add(r.duration_us as u64);
			totals.process_calls = totals.process_calls.saturating_add(1);
		}
	}

	(totals, aggregates.into_iter().collect())
}

fn render_flow_rows(out: &mut String, summary: &ProfileSummary, top_n: usize) {
	let (_, mut hot) = flow_aggregates(summary);
	hot.sort_by(|a, b| b.1.apply_us.cmp(&a.1.apply_us));
	hot.truncate(top_n);

	if hot.is_empty() {
		return;
	}

	let labels: Vec<String> = hot
		.iter()
		.map(|(key, _)| {
			let label = resolve_label(summary.interner.as_deref(), key.0);
			let id = resolve_id(summary.interner.as_deref(), key.1);
			format!("{}@{}", label, id)
		})
		.collect();
	let max_label_width = labels.iter().map(|s| s.len()).max().unwrap_or(0);

	for (i, (_, entry)) in hot.iter().enumerate() {
		let _ = writeln!(
			out,
			"    {:<width$}  apply={} calls={} lock={} io={}->{}",
			labels[i],
			fmt_us(entry.apply_us),
			entry.calls,
			fmt_us(entry.lock_us),
			entry.input_rows,
			entry.output_rows,
			width = max_label_width,
		);
	}
}

fn render_non_flow_rows(out: &mut String, summary: &ProfileSummary, cat: ProfileCategory, top_n: usize) {
	let mut agg: HashMap<u64, (u64, u64)> = HashMap::new();
	for r in &summary.records {
		if r.category_id != cat as u8 {
			continue;
		}
		let entry = agg.entry(r.callsite_id).or_insert((0, 0));
		entry.0 = entry.0.saturating_add(r.duration_us as u64);
		entry.1 = entry.1.saturating_add(1);
	}
	let mut sorted: Vec<(u64, (u64, u64))> = agg.into_iter().collect();
	sorted.sort_by(|a, b| b.1.0.cmp(&a.1.0));
	sorted.truncate(top_n);

	if sorted.is_empty() {
		return;
	}

	let labels: Vec<String> = sorted.iter().map(|(callsite, _)| format!("span#{}", callsite)).collect();
	let max_label_width = labels.iter().map(|s| s.len()).max().unwrap_or(0);

	for (i, (_, (total, calls))) in sorted.iter().enumerate() {
		let _ = writeln!(
			out,
			"    {:<width$}  total={} calls={}",
			labels[i],
			fmt_us(*total),
			calls,
			width = max_label_width,
		);
	}
}

fn resolve_label(interner: Option<&DimInterner>, idx: DimIdx) -> String {
	let resolved = interner.and_then(|i| i.resolve(idx));
	match resolved {
		Some(s) if !s.is_empty() => s,
		_ => "?".to_string(),
	}
}

fn resolve_id(interner: Option<&DimInterner>, idx: DimIdx) -> String {
	interner.and_then(|i| i.resolve(idx)).filter(|s| !s.is_empty()).unwrap_or_else(|| idx.to_string())
}

fn category_label(c: ProfileCategory) -> &'static str {
	match c {
		ProfileCategory::Query => "Query",
		ProfileCategory::Txn => "Txn",
		ProfileCategory::Storage => "Storage",
		ProfileCategory::Plan => "Plan",
		ProfileCategory::Cdc => "Cdc",
		ProfileCategory::Flow => "Flow",
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::{
		category::ProfileCategory,
		intern::DimInterner,
		percentile::PercentileHistogram,
		record::{AggregateRecord, DIM_UNSET, MAX_EXTRAS, MinimalSpanRecord},
		scope::ScopeId,
		summary::CategorySummary,
	};

	fn empty_summary() -> ProfileSummary {
		ProfileSummary {
			scope_id: ScopeId(1),
			scope_name: "x",
			started_at_nanos: 0,
			total_duration_us: 0,
			records: Vec::new(),
			per_category: [CategorySummary::default(); 6],
			interner: None,
		}
	}

	fn summary_with(records: Vec<MinimalSpanRecord>, interner: Option<Arc<DimInterner>>) -> ProfileSummary {
		ProfileSummary::from_records(ScopeId(1), "chaindex.batch_commit", 0, 12_345, records, interner)
	}

	#[test]
	fn empty_summary_renders_hot_empty() {
		let s = empty_summary();
		assert!(summary(&s, 5).ends_with(" hot=[]"));
	}

	#[test]
	fn summary_resolves_labels_when_interner_present() {
		let interner = Arc::new(DimInterner::new());
		let type_idx = interner.intern("map");
		let id_idx = interner.intern("n1");

		let apply_rec = MinimalSpanRecord::new(ProfileCategory::Flow, 100, 500)
			.with_dimensions([type_idx, id_idx])
			.with_extras([10, 7, 50, 0]);
		let s = summary_with(vec![apply_rec], Some(Arc::clone(&interner)));

		let line = summary(&s, 5);
		assert!(line.contains("map@n1=500us/50lk/1c/10in/7out"), "got {}", line);
	}

	#[test]
	fn summary_falls_back_to_placeholder_when_interner_missing() {
		let apply_rec = MinimalSpanRecord::new(ProfileCategory::Flow, 100, 500)
			.with_dimensions([42, 43])
			.with_extras([1, 1, 1, 0]);
		let s = summary_with(vec![apply_rec], None);

		let line = summary(&s, 5);
		assert!(line.contains("?@43=500us/1lk/1c/1in/1out"), "got {}", line);
	}

	#[test]
	fn summary_separates_process_and_apply() {
		let process_rec = MinimalSpanRecord::new(ProfileCategory::Flow, 200, 1000);
		let apply_rec = MinimalSpanRecord::new(ProfileCategory::Flow, 100, 400)
			.with_dimensions([1, 2])
			.with_extras([5, 3, 25, 0]);
		let s = summary_with(vec![process_rec, apply_rec], None);

		let line = summary(&s, 5);
		assert!(line.starts_with("tick(proc=1000us/1c, apply_sum=400us/1ops, lock_sum=25us)"), "got {}", line);
	}

	#[test]
	fn summary_aggregates_repeated_apply_per_operator() {
		let interner = Arc::new(DimInterner::new());
		let t = interner.intern("filter");
		let i = interner.intern("n2");

		let recs = vec![
			MinimalSpanRecord::new(ProfileCategory::Flow, 100, 100)
				.with_dimensions([t, i])
				.with_extras([5, 3, 10, 0]),
			MinimalSpanRecord::new(ProfileCategory::Flow, 100, 200)
				.with_dimensions([t, i])
				.with_extras([7, 5, 15, 0]),
		];
		let s = summary_with(recs, Some(Arc::clone(&interner)));

		let line = summary(&s, 5);
		assert!(line.contains("filter@n2=300us/25lk/2c/12in/8out"), "got {}", line);
	}

	#[test]
	fn summary_table_renders_multi_line_with_categories() {
		let interner = Arc::new(DimInterner::new());
		let map_t = interner.intern("map");
		let map_id = interner.intern("n1");
		let filter_t = interner.intern("filter");
		let filter_id = interner.intern("n2");

		let recs = vec![
			MinimalSpanRecord::new(ProfileCategory::Flow, 100, 5000)
				.with_dimensions([map_t, map_id])
				.with_extras([10, 7, 100, 0]),
			MinimalSpanRecord::new(ProfileCategory::Flow, 100, 3000)
				.with_dimensions([filter_t, filter_id])
				.with_extras([5, 3, 50, 0]),
			MinimalSpanRecord::new(ProfileCategory::Storage, 200, 1500),
			MinimalSpanRecord::new(ProfileCategory::Storage, 201, 600),
		];
		let s = summary_with(recs, Some(Arc::clone(&interner)));
		let table = summary_table(&s, 5);

		assert!(table.starts_with("profile scope=chaindex.batch_commit total="), "first line: {}", table);
		assert!(table.contains("Flow: 2 calls, apply="), "flow header missing: {}", table);
		assert!(table.contains("map@n1"), "map@n1 missing: {}", table);
		assert!(table.contains("filter@n2"), "filter@n2 missing: {}", table);
		assert!(table.contains("io=10->7"), "io rendering missing: {}", table);
		assert!(table.contains("Storage: 2 calls, total="), "storage header missing: {}", table);
		assert!(!table.contains('\u{2192}'), "unicode arrow leaked into ASCII output");
	}

	#[test]
	fn summary_table_aligns_labels_within_category() {
		let interner = Arc::new(DimInterner::new());
		let short = interner.intern("a");
		let long = interner.intern("very_long_type");
		let short_id = interner.intern("1");
		let long_id = interner.intern("z");

		let recs = vec![
			MinimalSpanRecord::new(ProfileCategory::Flow, 100, 100)
				.with_dimensions([short, short_id])
				.with_extras([0, 0, 0, 0]),
			MinimalSpanRecord::new(ProfileCategory::Flow, 100, 200)
				.with_dimensions([long, long_id])
				.with_extras([0, 0, 0, 0]),
		];
		let s = summary_with(recs, Some(Arc::clone(&interner)));
		let table = summary_table(&s, 5);

		let lines: Vec<&str> = table.lines().collect();
		let short_line = lines.iter().find(|l| l.contains("a@1 ")).expect("short label line");
		let long_line = lines.iter().find(|l| l.contains("very_long_type@z")).expect("long label line");
		let short_apply_pos = short_line.find("apply=").unwrap();
		let long_apply_pos = long_line.find("apply=").unwrap();
		assert_eq!(
			short_apply_pos, long_apply_pos,
			"apply= columns are not aligned:\n{}\n{}",
			short_line, long_line
		);
	}

	#[test]
	fn fmt_us_unit_promotion() {
		assert_eq!(fmt_us(0), "0us");
		assert_eq!(fmt_us(500), "500us");
		assert_eq!(fmt_us(1_500), "1.5ms");
		assert_eq!(fmt_us(12_345), "12.3ms");
		assert_eq!(fmt_us(1_500_000), "1.5s");
	}

	#[test]
	fn aggregates_table_renders_per_category() {
		let records = vec![
			AggregateRecord {
				category: ProfileCategory::Flow,
				span_name: "flow::engine::process_batch".to_string(),
				dimensions: Vec::new(),
				calls: 6,
				total_us: 1_000,
				histogram: PercentileHistogram::new(),
				extras_sum: [0; MAX_EXTRAS],
			},
			AggregateRecord {
				category: ProfileCategory::Flow,
				span_name: "flow::engine::apply".to_string(),
				dimensions: vec!["map".to_string(), "n1".to_string()],
				calls: 3,
				total_us: 5_000,
				histogram: PercentileHistogram::new(),
				extras_sum: [0; MAX_EXTRAS],
			},
			AggregateRecord {
				category: ProfileCategory::Flow,
				span_name: "flow::engine::apply".to_string(),
				dimensions: vec!["filter".to_string(), "n2".to_string()],
				calls: 2,
				total_us: 3_000,
				histogram: PercentileHistogram::new(),
				extras_sum: [0; MAX_EXTRAS],
			},
			AggregateRecord {
				category: ProfileCategory::Storage,
				span_name: "store::multi::write".to_string(),
				dimensions: Vec::new(),
				calls: 30,
				total_us: 1_500,
				histogram: PercentileHistogram::new(),
				extras_sum: [0; MAX_EXTRAS],
			},
		];
		let table = aggregates_table(&records, 10);
		assert!(table.starts_with("profile (accumulator) 4 records, 41 calls, total="));
		assert!(table.contains("Flow: 3 records, 11 calls, total="));
		assert!(table.contains("flow::engine::apply [2 ops, total="));
		assert!(table.contains("\n      map@n1  "), "expected nested map@n1 row, got:\n{}", table);
		assert!(table.contains("\n      filter@n2  "), "expected nested filter@n2 row, got:\n{}", table);
		assert!(table.contains("\n    flow::engine::process_batch  total="));
		assert!(table.contains("Storage: 1 records, 30 calls, total="));
		assert!(table.contains("\n    store::multi::write  total="));
	}

	#[test]
	fn aggregates_table_groups_flow_apply_by_operator() {
		let mk = |op: &str, total: u64| AggregateRecord {
			category: ProfileCategory::Flow,
			span_name: "flow::engine::apply".to_string(),
			dimensions: vec![op.to_string()],
			calls: 1,
			total_us: total,
			histogram: PercentileHistogram::new(),
			extras_sum: [0; MAX_EXTRAS],
		};
		let records = vec![mk("op_a", 4_000), mk("op_b", 3_000), mk("op_c", 2_000), mk("op_d", 1_000)];
		let table = aggregates_table(&records, 2);
		assert!(table.contains("flow::engine::apply [4 ops, total="));
		assert!(table.contains("\n      op_a  "));
		assert!(table.contains("\n      op_b  "));
		assert!(!table.contains("\n      op_c  "), "op_c should be truncated by top_n=2: {}", table);
		assert!(!table.contains("\n      op_d  "), "op_d should be truncated by top_n=2: {}", table);
	}

	#[test]
	fn aggregates_table_single_no_dim_record_renders_inline() {
		let records = vec![AggregateRecord {
			category: ProfileCategory::Flow,
			span_name: "flow::engine::process_batch".to_string(),
			dimensions: Vec::new(),
			calls: 5,
			total_us: 800,
			histogram: PercentileHistogram::new(),
			extras_sum: [0; MAX_EXTRAS],
		}];
		let table = aggregates_table(&records, 10);
		assert!(!table.contains("[1 ops"), "single no-dim record must render inline, got:\n{}", table);
		assert!(table.contains("\n    flow::engine::process_batch  total="));
	}

	#[test]
	fn aggregates_table_handles_empty() {
		let table = aggregates_table(&[], 10);
		assert!(table.contains("empty"));
	}

	#[test]
	fn summary_table_skips_empty_categories() {
		let recs =
			vec![MinimalSpanRecord::new(ProfileCategory::Flow, 100, 0)
				.with_dimensions([DIM_UNSET, DIM_UNSET])];
		let s = summary_with(recs, None);
		let table = summary_table(&s, 5);
		assert!(table.contains("Flow:"));
		assert!(!table.contains("Query:"));
		assert!(!table.contains("Storage:"));
	}
}
