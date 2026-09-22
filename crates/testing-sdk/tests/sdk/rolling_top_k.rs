// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, collections::BTreeMap};

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowRequirements, WindowSize, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
	row::Row as CoreRow,
	state::timer::TimerKind,
};
use reifydb_flow::window::{
	accumulator::invertible::{keyed::KeyedInvertibleAccumulator, moments::Moments},
	span::WindowSpan,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		extern_c::binding::operator::ExternCOperatorAdapter,
		view::RowView,
		windowed::{
			operator::{AllKinds, Emit, WindowSettings, WindowedOperator},
			top_k::TopKDriver,
		},
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	harness::ExternCOperatorHarnessBuilder,
};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::millis,
	value::{Value, datetime::DateTime, diff_type::DiffType, value_type::ValueType},
};

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

impl OperatorMetadata for TestTopVolume {
	const NAME: &'static str = "test_top_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TestTopVolume {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = KeyedInvertibleAccumulator<u64, Moments>;
	type Output = BTreeMap<u32, TopOut>;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, f64))> {
		let group = row.utf8("group")?.to_string();
		let trader = row.u64("trader")?;
		let volume = row.f64("volume")?;
		Some((group, (trader, volume)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> KeyedInvertibleAccumulator<u64, Moments> {
		KeyedInvertibleAccumulator::default()
	}
}

impl Emit for TestTopVolume {
	type Kinds = AllKinds;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, Moments>,
	) -> Option<BTreeMap<u32, TopOut>> {
		let mut ranked: Vec<(u64, f64)> =
			value.iter().map(|(trader, moments)| (*trader, moments.sum())).collect();
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
		Some(out)
	}
}

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("window_start", ValueType::Uint8),
		RowShapeField::unconstrained("trader", ValueType::Uint8),
		RowShapeField::unconstrained("volume", ValueType::Float8),
	]
}

fn input_row(rn: u64, group: &str, window_start: u64, trader: u64, volume: f64) -> CoreRow {
	// #time is stamped from the same coordinate the fixture buckets on, so these tests assert
	// the same thing before and after the window coordinate moves onto #time. Leaving it
	// unstamped would park every row at the epoch and collapse all windows into one bucket.
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![
			Value::Utf8(group.into()),
			Value::Uint8(window_start),
			Value::Uint8(trader),
			Value::float8(volume),
		])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(window_start))
		.build()
}

fn window_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(3)),
			lag: None,
			pane: Some(millis(1)),
		}),
		lateness: Some(WithSpan::Duration(millis(3_600_000))),
		immutable: None,
		retention: None,
	}
}

fn sealed_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(3)),
			lag: None,
			pane: Some(millis(1)),
		}),
		lateness: Some(WithSpan::Duration(millis(117))),
		immutable: None,
		retention: None,
	}
}

fn dead_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(3)),
			lag: None,
			pane: Some(millis(1)),
		}),
		lateness: Some(WithSpan::Duration(millis(10))),
		immutable: None,
		retention: None,
	}
}

#[test]
fn same_window_volume_accumulates_per_trader() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(window_with())
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(window_with())
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "BTC", 1, 200, 9.0))
			.insert(input_row(3, "BTC", 2, 300, 7.0))
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
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "BTC", 1, 200, 9.0))
			.build())
		.expect("apply");
	// Emptying the newest window drops it from the buffer, which shifts rank 1 and leaves
	// rank 2 with nothing to name - that vacancy has to surface as a Remove.
	let out = h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 1, 200, 9.0)).build()).expect("apply");
	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert!(kinds.contains(&DiffType::Update), "rank-1 changed identity, expect Update");
	assert!(kinds.contains(&DiffType::Remove), "rank-2 vanished, expect Remove");
}

#[test]
fn time_eviction_drops_oldest_window() {
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	// A fourth window pushes window 0 out of the 3 ms span, so window 0 and trader 100 with it must go.
	// Trader 100 carries the largest volume of the four on purpose: it outranks everyone while
	// window 0 is still buffered, so the assertions below can only hold once eviction has run.
	// With a smaller volume the expected top-2 would be identical whether or not anything was
	// evicted, and the test would pass against a driver that never buckets or never evicts.
	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 9.0))
			.insert(input_row(2, "BTC", 1, 200, 8.0))
			.insert(input_row(3, "BTC", 2, 300, 2.0))
			.insert(input_row(4, "BTC", 3, 400, 5.0))
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
fn buried_window_insert_accepted_while_lateness_is_open() {
	// while the lateness window has not elapsed, an insert into an older coordinate must still merge
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 2, 100, 5.0)).build()).expect("apply");
	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 1, 999, 999.0)).build()).expect("apply");
	assert!(!out.diffs.is_empty(), "ungated rolling-top-k driver accepts late events");
}

struct SealedTopVolume;

impl OperatorMetadata for SealedTopVolume {
	const NAME: &'static str = "sealed_top_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for SealedTopVolume {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = KeyedInvertibleAccumulator<u64, Moments>;
	type Output = BTreeMap<u32, TopOut>;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, f64))> {
		TestTopVolume.extract(ctx, row)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> KeyedInvertibleAccumulator<u64, Moments> {
		KeyedInvertibleAccumulator::default()
	}
}

impl Emit for SealedTopVolume {
	type Kinds = AllKinds;

	fn build_output(
		&self,
		group: &String,
		span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, Moments>,
	) -> Option<BTreeMap<u32, TopOut>> {
		TestTopVolume.build_output(group, span, value)
	}
}

#[test]
fn a_stopped_feed_still_drains_group_meta_on_the_seal_timer() {
	// A group that stops reporting must still be reclaimed, or a high-cardinality group key
	// grows without bound; nothing moves here after the initial batch except the watermark.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<SealedTopVolume>>>::new()
		.with(sealed_with())
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
fn a_rolling_top_k_time_window_arms_a_seal_timer() {
	// a driver with a required window must always acquire a seal retention policy
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(window_with())
		.build()
		.expect("harness");
	let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 7, 10.0)).build()).expect("apply");

	assert!(!h.armed_timers().is_empty(), "a windowed operator must arm a seal timer on its first insert");
}

#[test]
fn create_without_a_window_reports_flow_065() {
	// require_window must refuse a missing window before any row reaches the aggregator
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(ApplyWith::default())
		.build()
	else {
		panic!("create must refuse a missing window");
	};
	assert!(err.to_string().contains("FLOW_065"), "expected FLOW_065, got: {err}");
}

#[test]
fn create_with_the_wrong_window_kind_reports_flow_066() {
	// require_window must refuse a window kind this driver does not support
	let with = ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60)),
		}),
		lateness: None,
		immutable: None,
		retention: None,
	};
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(with)
		.build()
	else {
		panic!("create must refuse an unsupported window kind");
	};
	assert!(err.to_string().contains("FLOW_066"), "expected FLOW_066, got: {err}");
}

#[test]
fn a_top_k_operator_publishes_rolling_only_and_needs_pane() {
	// a top-k that hid its pane need would pass create and then fail every window at runtime
	assert_eq!(
		<TopKDriver<TestTopVolume> as MountedOperator>::WINDOW,
		WindowRequirements {
			takes_window: true,
			kinds: &["rolling"],
			domain: WindowSizeDomain::Time,
			needs_pane: true,
		}
	);
}

#[test]
fn a_dead_top_k_group_removes_every_ranked_row_on_its_timer() {
	// A dead group must remove every ranked row, otherwise the sink keeps a ranking nobody updates.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(dead_with())
		.build()
		.expect("harness");
	h.apply(TestChangeBuilder::new()
		.insert(input_row(1, "BTC", 100, 7, 10.0))
		.insert(input_row(2, "BTC", 100, 8, 5.0))
		.build())
		.expect("apply");
	let dead: Vec<(DateTime, TimerKind)> =
		h.armed_timers().into_iter().filter(|t| t.key == b"rolling-dead").map(|t| (t.due, t.kind)).collect();
	assert_eq!(dead, vec![(DateTime::from_millis(117), TimerKind::Seal)]);

	let out = h
		.on_timer(DateTime::from_millis(117), TimerKind::Seal, b"rolling-dead")
		.expect("timer")
		.expect("the dead group must emit");

	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert_eq!(kinds, vec![DiffType::Remove]);
	let pre = out.diffs[0].pre().expect("pre");
	let by_rank: BTreeMap<u32, (u64, f64)> = (0..pre.row_count())
		.map(|i| {
			let r = pre.row_ref(i).expect("row");
			(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
		})
		.collect();
	assert_eq!(by_rank, BTreeMap::from([(1, (7, 10.0)), (2, (8, 5.0))]));
}

#[test]
fn a_top_k_group_revived_inside_its_window_keeps_its_old_panes() {
	// A group must live one size past its horizon, otherwise a revived row loses the panes still in its window.
	let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<TestTopVolume>>>::new()
		.with(dead_with())
		.build()
		.expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 7, 1.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(115)).expect("watermark");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 102, 7, 2.0)).build()).expect("apply");

	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert_eq!(kinds, vec![DiffType::Update]);
	let post = out.diffs[0].post().expect("post");
	let r = post.row_ref(0).expect("row");
	assert_eq!((post.row_count(), r.u64("trader").unwrap(), r.f64("volume").unwrap()), (1, 7, 3.0));
}
