// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, collections::BTreeMap};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::{
	encoded::shape::{RowShape, RowShapeField},
	key::encoded::EncodedKey,
};
use reifydb_core::{interface::catalog::flow::OperatorId, metrics::heap::HeapSize, row::Row as CoreRow};
use reifydb_flow::window::{
	accumulator::{
		WindowAccumulator,
		invertible::{KeyedInvertibleAccumulator, Moments},
	},
	span::WindowCoord,
};
use reifydb_sdk::{
	config::Config,
	error::Result,
	operator::{
		FFIOperatorAdapter, column::operator::OperatorColumn, context::OperatorContext, view::RowView,
		windowed::multi_rolling::*,
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestRowBuilder},
	harness::FFIOperatorHarnessBuilder,
};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType};

// Rolling top-2 traders by summed volume. Each window cell is keyed and invertible so an
// Update or Remove subtracts a trade's volume rather than dropping the whole window.

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct TopOut {
	group: String,
	rank: u32,
	trader: u64,
	volume: f64,
}

row!(TopOut {
	group: String,
	rank: u32,
	trader: u64,
	volume: f64
});

struct TestTopVolume;

impl MultiRollingOperator for TestTopVolume {
	type GroupKey = String;
	type WindowSlot = u64;
	type Accumulator = KeyedInvertibleAccumulator<u64, Moments>;
	type SecondaryKey = u32;
	type Output = TopOut;

	fn capacity(&self) -> usize {
		3
	}

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, (u64, f64))> {
		let group = row.utf8("group")?.to_string();
		let window_start = row.u64("window_start")?;
		let trader = row.u64("trader")?;
		let volume = row.f64("volume")?;
		Some((group, window_start, (trader, volume)))
	}

	fn combine(
		&self,
		group: &String,
		buffer: &BTreeMap<u64, KeyedInvertibleAccumulator<u64, Moments>>,
	) -> BTreeMap<u32, TopOut> {
		let mut totals: BTreeMap<u64, f64> = BTreeMap::new();
		for window in buffer.values() {
			if let Some(per_trader) = window.finalize() {
				for (trader, moments) in per_trader {
					*totals.entry(trader).or_insert(0.0) += moments.sum();
				}
			}
		}
		let mut ranked: Vec<(u64, f64)> = totals.into_iter().collect();
		ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
		let mut out = BTreeMap::new();
		for (i, (trader, volume)) in ranked.into_iter().take(2).enumerate() {
			let rank = (i as u32) + 1;
			out.insert(
				rank,
				TopOut {
					group: group.clone(),
					rank,
					trader,
					volume,
				},
			);
		}
		out
	}
}

impl MultiRollingRegistration for TestTopVolume {
	const NAME: &'static str = "test_top_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_state_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str("state").str(group).build()
	}

	fn encode_row_key(&self, group: &String, secondary: &u32) -> EncodedKey {
		EncodedKey::builder().str("row").str(group).u32(*secondary).build()
	}
}

fn input_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("window_start", ValueType::Uint8),
		RowShapeField::unconstrained("trader", ValueType::Uint8),
		RowShapeField::unconstrained("volume", ValueType::Float8),
	])
}

fn input_row(rn: u64, group: &str, window_start: u64, trader: u64, volume: f64) -> CoreRow {
	// #time is stamped from the same coordinate the fixture buckets on, so these tests assert
	// the same thing before and after the window coordinate moves onto #time. Leaving it
	// unstamped would park every row at the epoch and collapse all windows into one bucket.
	TestRowBuilder::new(rn)
		.with_values(vec![
			Value::Utf8(group.into()),
			Value::Uint8(window_start),
			Value::Uint8(trader),
			Value::float8(volume),
		])
		.with_shape(input_shape())
		.with_time(DateTime::from_millis(window_start))
		.build()
}

#[test]
fn same_window_volume_accumulates_per_trader() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
		.build()
		.expect("harness");
	// Two trades for the same trader in one window must sum, not overwrite each other.
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "BTC", 0, 200, 9.0))
			.insert(input_row(3, "BTC", 0, 100, 3.0))
			.build())
		.expect("apply");
	let post = out.diffs[0].post().expect("post");
	let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
		.map(|i| {
			let r = post.row_ref(i).expect("row");
			(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
		})
		.collect();
	assert_eq!(by_rank.get(&1).copied(), Some((200u64, 9.0)), "trader 200 leads at 9.0");
	assert_eq!(by_rank.get(&2).copied(), Some((100u64, 8.0)), "trader 100 volume summed 5+3 = 8.0");
}

#[test]
fn update_subtracts_old_volume_no_double_count() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "BTC", 0, 200, 9.0))
			.build())
		.expect("apply");
	// The update must route remove(5)+add(20), so trader 100 lands on 20 and not 25.
	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 0, 100, 5.0), input_row(1, "BTC", 0, 100, 20.0))
			.build())
		.expect("apply");
	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert!(kinds.contains(&DiffType::Update), "ranks changed, expect Update");
	let post = out.diffs.iter().find(|d| d.kind() == DiffType::Update).unwrap().post().expect("post");
	let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
		.map(|i| {
			let r = post.row_ref(i).expect("row");
			(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
		})
		.collect();
	assert_eq!(by_rank.get(&1).copied(), Some((100u64, 20.0)), "trader 100 now leads at 20, not 25");
}

#[test]
fn top_2_across_three_windows() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "BTC", 60, 200, 9.0))
			.insert(input_row(3, "BTC", 120, 300, 7.0))
			.build())
		.expect("apply");
	let post = out.diffs[0].post().expect("post");
	assert_eq!(post.row_count(), 2);
	let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
		.map(|i| {
			let r = post.row_ref(i).expect("row");
			(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
		})
		.collect();
	assert_eq!(by_rank.get(&1).copied(), Some((200u64, 9.0)));
	assert_eq!(by_rank.get(&2).copied(), Some((300u64, 7.0)));
}

#[test]
fn vanishing_rank_emits_remove_at_high_water() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "BTC", 60, 200, 9.0))
			.build())
		.expect("apply");
	// Emptying the newest window drops it from the buffer, which shifts rank 1 and leaves
	// rank 2 with nothing to name - that vacancy has to surface as a Remove.
	let out = h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 60, 200, 9.0)).build()).expect("apply");
	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert!(kinds.contains(&DiffType::Update), "rank-1 changed identity, expect Update");
	assert!(kinds.contains(&DiffType::Remove), "rank-2 vanished, expect Remove");
}

#[test]
fn capacity_eviction_drops_oldest_window() {
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
		.build()
		.expect("harness");
	// A fourth window exceeds the capacity of 3, so window 0 and trader 100 with it must go.
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 1.0))
			.insert(input_row(2, "BTC", 60, 200, 8.0))
			.insert(input_row(3, "BTC", 120, 300, 2.0))
			.insert(input_row(4, "BTC", 180, 400, 5.0))
			.build())
		.expect("apply");
	let post = out.diffs[0].post().expect("post");
	let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
		.map(|i| {
			let r = post.row_ref(i).expect("row");
			(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
		})
		.collect();
	assert_eq!(by_rank.get(&1).copied(), Some((200u64, 8.0)));
	assert_eq!(by_rank.get(&2).copied(), Some((400u64, 5.0)), "window 0 evicted; trader 100 gone");
}

#[test]
fn buried_window_insert_accepted_without_sealing() {
	// Without a seal envelope there is no implicit high-water drop, so an insert into an older
	// coordinate merges rather than being discarded.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 100, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 999, 999.0)).build()).expect("apply");
	assert!(!out.diffs.is_empty(), "ungated multi-rolling driver accepts late events");
}

fn millis(value: u64) -> Duration {
	Duration::from_milliseconds_const(value as i64)
}

struct SealedTopVolume;

impl MultiRollingOperator for SealedTopVolume {
	type GroupKey = String;
	type WindowSlot = DateTime;
	type Accumulator = KeyedInvertibleAccumulator<u64, Moments>;
	type SecondaryKey = u32;
	type Output = TopOut;

	fn capacity(&self) -> usize {
		3
	}

	fn seal_after(&self) -> Option<Duration> {
		Some(millis(120))
	}

	fn extract(
		&self,
		ctx: &mut impl OperatorContext,
		row: &impl RowView,
	) -> Option<(String, DateTime, (u64, f64))> {
		let (group, window_start, contribution) = TestTopVolume.extract(ctx, row)?;
		Some((group, DateTime::from_millis(window_start), contribution))
	}

	fn combine(
		&self,
		group: &String,
		buffer: &BTreeMap<DateTime, KeyedInvertibleAccumulator<u64, Moments>>,
	) -> BTreeMap<u32, TopOut> {
		let reindexed: BTreeMap<u64, KeyedInvertibleAccumulator<u64, Moments>> =
			buffer.iter().map(|(coord, acc)| (coord.to_order(), acc.clone())).collect();
		TestTopVolume.combine(group, &reindexed)
	}
}

impl MultiRollingRegistration for SealedTopVolume {
	const NAME: &'static str = "sealed_top_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn encode_state_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str("state").str(group).build()
	}

	fn encode_row_key(&self, group: &String, secondary: &u32) -> EncodedKey {
		EncodedKey::builder().str("row").str(group).u32(*secondary).build()
	}
}

#[test]
fn a_stopped_feed_still_drains_group_meta_on_the_seal_timer() {
	// A group that stops reporting must still be reclaimed, or a high-cardinality group key
	// grows without bound; nothing moves here after the initial batch except the watermark.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<SealedTopVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 7, 10.0))
			.insert(input_row(2, "ETH", 0, 8, 50.0))
			.build())
		.expect("apply");
	let before = h.snapshot_state().len();

	let fired = h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");

	assert!(fired > 0, "the insert must have armed a seal timer that the watermark then passes");
	assert!(
		h.snapshot_state().len() < before,
		"a fired seal timer must reclaim the meta of groups that stopped reporting, but the \
		 store went from {before} rows to {}",
		h.snapshot_state().len()
	);
}

#[test]
fn an_ungated_multi_rolling_operator_arms_no_seal_timer() {
	// An operator that never opted into sealing must not acquire a retention policy.
	let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 7, 10.0)).build()).expect("apply");

	assert!(h.armed_timers().is_empty(), "an operator with seal_after = None must arm no timer");
}
