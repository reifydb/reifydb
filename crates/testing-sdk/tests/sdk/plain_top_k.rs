// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::TypeId, cmp::Ordering, collections::BTreeMap};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::shape::RowShapeField,
};
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	key::operator::state::GroupId,
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
	row::Row as CoreRow,
};
use reifydb_flow::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::{MergeAccumulator, WindowAccumulator},
		settings::WindowSettings,
		span::WindowSpan,
	},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		extern_c::binding::operator::ExternCOperatorAdapter,
		view::RowView,
		windowed::{
			operator::{AllKinds, Emit, PlainMarker, TopKMarker, WindowDriver, WindowedOperator},
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

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, PartialEq, HeapSize)]
struct TraderVolumes {
	volumes: BTreeMap<u64, f64>,
}

impl WindowAccumulator for TraderVolumes {
	type Contribution = (u64, f64);
	type Output = BTreeMap<u64, f64>;

	fn add(&mut self, (trader, volume): &(u64, f64)) {
		*self.volumes.entry(*trader).or_insert(0.0) += volume;
	}

	fn remove(&mut self, (trader, volume): &(u64, f64)) {
		if let Some(total) = self.volumes.get_mut(trader) {
			*total -= volume;
			if *total == 0.0 {
				self.volumes.remove(trader);
			}
		}
	}

	fn finalize(&self) -> Option<BTreeMap<u64, f64>> {
		(!self.volumes.is_empty()).then(|| self.volumes.clone())
	}

	fn is_empty(&self) -> bool {
		self.volumes.is_empty()
	}
}

impl MergeAccumulator for TraderVolumes {
	fn merge(&mut self, other: &Self) {
		for (trader, volume) in &other.volumes {
			*self.volumes.entry(*trader).or_insert(0.0) += volume;
		}
	}
}

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct TopOut {
	group: String,
	rank: u32,
	trader: u64,
	volume: f64,
	start: u64,
	end: u64,
}

row!(TopOut {
	group: String,
	rank: u32,
	trader: u64,
	volume: f64,
	start: u64,
	end: u64
});

fn ranked(group: &str, span: WindowSpan<DateTime>, volumes: &BTreeMap<u64, f64>) -> BTreeMap<u32, TopOut> {
	let mut totals: Vec<(u64, f64)> = volumes.iter().map(|(trader, volume)| (*trader, *volume)).collect();
	totals.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
	totals.into_iter()
		.take(2)
		.enumerate()
		.map(|(i, (trader, volume))| {
			let rank = i as u32 + 1;
			(
				rank,
				TopOut {
					group: group.to_string(),
					rank,
					trader,
					volume,
					start: span.start.to_order(),
					end: span.end.to_order(),
				},
			)
		})
		.collect()
}

macro_rules! volume_operator {
	($name:ident, $output:ty, $build:expr) => {
		struct $name;

		impl OperatorMetadata for $name {
			const NAME: &'static str = "top_volume";
			const VERSION: &'static str = "0.0.1";
			const DESCRIPTION: &'static str = "test fixture";
			const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
			const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
			const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
		}

		impl WindowedOperator for $name {
			type Coord = DateTime;
			type GroupKey = String;
			type Accumulator = TraderVolumes;
			type Output = $output;

			fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
				Ok(Self)
			}

			fn coord(&self, row: &impl RowView) -> Result<Option<DateTime>> {
				Ok(row.row_time())
			}

			fn extract(
				&self,
				_: &mut impl GuestContext<Windowed>,
				row: &impl RowView,
			) -> Result<Option<(String, (u64, f64))>> {
				let (Some(group), Some(trader), Some(volume)) =
					(row.utf8("group")?, row.u64("trader")?, row.f64("volume")?)
				else {
					return Ok(None);
				};
				Ok(Some((group.to_string(), (trader, volume))))
			}

			fn new_accumulator(&self, _: &WindowSettings<DateTime>) -> TraderVolumes {
				TraderVolumes::default()
			}
		}

		impl Emit for $name {
			type Kinds = AllKinds;

			fn build_output(
				&self,
				group: &String,
				span: WindowSpan<DateTime>,
				value: &BTreeMap<u64, f64>,
			) -> Option<$output> {
				$build(group, span, value)
			}
		}
	};
}

volume_operator!(
	TopVolume,
	BTreeMap<u32, TopOut>,
	|group: &String, span: WindowSpan<DateTime>, value: &BTreeMap<u64, f64>| Some(ranked(group, span, value))
);
volume_operator!(
	CappedVolume,
	BTreeMap<u32, TopOut>,
	|group: &String, span: WindowSpan<DateTime>, value: &BTreeMap<u64, f64>| (value.values().sum::<f64>() <= 100.0)
		.then(|| ranked(group, span, value))
);
volume_operator!(LeaderVolume, TopOut, |group: &String, span: WindowSpan<DateTime>, value: &BTreeMap<u64, f64>| {
	ranked(group, span, value).remove(&1)
});

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("trader", ValueType::Uint8),
		RowShapeField::unconstrained("volume", ValueType::Float8),
	]
}

fn input_row(rn: u64, group: &str, ts: u64, trader: u64, volume: f64) -> CoreRow {
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(trader), Value::float8(volume)])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(ts as i64))
		.build()
}

fn rolling(size: u64, pane: Option<u64>, lateness: u64) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(size)),
			lag: None,
			pane: pane.map(millis),
		}),
		lateness: Some(WithSpan::Duration(millis(lateness))),
		immutable: None,
		retention: None,
	}
}

type Emitted = Vec<(DiffType, u32, u64, f64)>;

fn render(out: &Change) -> Emitted {
	let mut rendered = Vec::new();
	for diff in &out.diffs {
		let rows = match diff.kind() {
			DiffType::Remove => diff.pre().expect("pre"),
			_ => diff.post().expect("post"),
		};
		for i in 0..rows.row_count() {
			let r = rows.row_ref(i).expect("row");
			rendered.push((
				diff.kind(),
				r.u32("rank").expect("rank"),
				r.u64("trader").expect("trader"),
				r.f64("volume").expect("volume"),
			));
		}
	}
	rendered.sort_by(|a, b| (a.0 as u8, a.1).cmp(&(b.0 as u8, b.1)));
	rendered
}

macro_rules! harness {
	($driver:ty, $with:expr) => {
		ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<TopKDriver<$driver>>>::new().with($with).build()
	};
}

fn marker_of<T: WindowDriver<M>, M: 'static>() -> TypeId {
	TypeId::of::<M>()
}

#[test]
fn an_operator_whose_output_is_one_row_picks_the_plain_marker_and_a_ranked_map_picks_top_k() {
	// Both blanket impls must never apply to one operator, or registration cannot infer the driver.
	assert_eq!(marker_of::<LeaderVolume, _>(), TypeId::of::<PlainMarker>());
	assert_eq!(marker_of::<TopVolume, _>(), TypeId::of::<TopKMarker>());
}

#[test]
fn a_trader_summed_across_panes_outranks_a_leader_of_any_single_pane() {
	// Panes must be merged before ranking; ranking the newest pane alone would crown trader 200.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 100, 6.0)).build()).expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(2, "BTC", 1, 100, 6.0))
			.insert(input_row(3, "BTC", 1, 200, 9.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Insert, 2, 200, 9.0), (DiffType::Update, 1, 100, 12.0)],
		"trader 100 holds 6 + 6 over two panes, ahead of trader 200's 9"
	);
}

#[test]
fn a_first_batch_inserts_one_row_per_rank() {
	// Each rank is its own row; collapsing the map into one row would lose every rank but one.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 3_600_000)).expect("harness");

	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "BTC", 0, 200, 9.0))
			.insert(input_row(3, "BTC", 0, 300, 1.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Insert, 1, 200, 9.0), (DiffType::Insert, 2, 100, 5.0)],
		"only the top two ranks are emitted"
	);
}

#[test]
fn groups_rank_independently() {
	// The row key starts with the group; without it two groups' rank 1 would share one row.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 3_600_000)).expect("harness");

	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 5.0))
			.insert(input_row(2, "ETH", 0, 200, 9.0))
			.build())
		.expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Insert, 1, 100, 5.0), (DiffType::Insert, 1, 200, 9.0)]);
}

#[test]
fn an_update_subtracts_the_old_volume_and_adds_the_new() {
	// An update is a remove plus an add; skipping the remove would leave trader 100 on 25.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new()
		.insert(input_row(1, "BTC", 0, 100, 5.0))
		.insert(input_row(2, "BTC", 0, 200, 9.0))
		.build())
		.expect("apply");

	let out = h
		.apply(TestChangeBuilder::new()
			.update(input_row(1, "BTC", 0, 100, 5.0), input_row(1, "BTC", 0, 100, 20.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Update, 1, 100, 20.0), (DiffType::Update, 2, 200, 9.0)],
		"trader 100 now leads on 20"
	);
}

#[test]
fn a_rank_left_without_a_trader_is_removed() {
	// Emptying the newest pane leaves rank 2 with nothing to name; that vacancy must surface as a Remove.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new()
		.insert(input_row(1, "BTC", 0, 100, 5.0))
		.insert(input_row(2, "BTC", 1, 200, 9.0))
		.build())
		.expect("apply");

	let out = h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 1, 200, 9.0)).build()).expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Update, 1, 100, 5.0), (DiffType::Remove, 2, 100, 5.0)],
		"rank 1 changes hands and rank 2 is vacated"
	);
}

#[test]
fn panes_beyond_the_capacity_are_evicted_from_the_ranking() {
	// Trader 100 leads while pane 0 is buffered; it can only vanish from the ranking once eviction ran.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 3_600_000)).expect("harness");

	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 0, 100, 9.0))
			.insert(input_row(2, "BTC", 1, 200, 8.0))
			.insert(input_row(3, "BTC", 2, 300, 2.0))
			.insert(input_row(4, "BTC", 3, 400, 5.0))
			.build())
		.expect("apply");

	assert_eq!(
		render(&out),
		vec![(DiffType::Insert, 1, 200, 8.0), (DiffType::Insert, 2, 400, 5.0)],
		"a fourth pane over capacity 3 evicts pane 0 and trader 100 with it"
	);
}

#[test]
fn a_group_whose_output_turns_none_has_its_rows_withdrawn() {
	// None means nothing to rank; the rows already emitted must be retracted, not left standing.
	let mut h = harness!(CappedVolume, rolling(3, Some(1), 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new()
		.insert(input_row(1, "BTC", 0, 100, 5.0))
		.insert(input_row(2, "BTC", 0, 200, 9.0))
		.build())
		.expect("apply");

	let out = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 1, 300, 500.0)).build()).expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Remove, 1, 200, 9.0), (DiffType::Remove, 2, 100, 5.0)]);
}

#[test]
fn a_rolling_view_without_a_pane_reports_flow_075() {
	// Without a pane the engine cannot bucket rows or size the buffer; it must fail at create, not run blind.
	let err = harness!(TopVolume, rolling(3, None, 3_600_000)).err().expect("create must fail");

	assert!(err.to_string().contains("FLOW_075"), "expected FLOW_075, got: {err}");
}

#[test]
fn create_with_a_tumbling_window_reports_flow_066() {
	// Top-k is rolling only; running it tumbling would rank a window it never merges.
	let with = ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60)),
		}),
		lateness: None,
		immutable: None,
		retention: None,
	};

	let err = harness!(TopVolume, with).err().expect("create must fail");

	assert!(err.to_string().contains("FLOW_066"), "expected FLOW_066, got: {err}");
}

#[test]
fn create_without_a_window_reports_flow_065() {
	// A missing window must be refused before any row reaches the aggregator.
	let err = harness!(TopVolume, ApplyWith::default()).err().expect("create must fail");

	assert!(err.to_string().contains("FLOW_065"), "expected FLOW_065, got: {err}");
}

#[test]
fn a_rolling_top_k_time_window_arms_a_seal_timer() {
	// A driver with a lateness must always acquire a seal retention policy.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 117)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 7, 10.0)).build()).expect("apply");

	assert!(!h.armed_timers().is_empty(), "a windowed operator must arm a seal timer on its first insert");
}

#[test]
fn a_stopped_feed_still_drains_group_meta_on_the_seal_timer() {
	// A group that stops reporting must still be reclaimed, or a high-cardinality group key grows without bound.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 117)).expect("harness");
	h.apply(TestChangeBuilder::new()
		.insert(input_row(1, "BTC", 0, 7, 10.0))
		.insert(input_row(2, "ETH", 0, 8, 50.0))
		.build())
		.expect("apply");
	let before = h.snapshot_state().len();

	let fired = h.advance_watermark(DateTime::from_millis(10_000)).expect("advance watermark");

	assert!(fired > 0, "the insert must have armed a seal timer that the watermark then passes");
	assert!(
		h.snapshot_state().len() < before,
		"a fired seal timer must reclaim the meta of groups that stopped reporting, but the store went from {before} rows to {}",
		h.snapshot_state().len()
	);
}

#[test]
fn a_row_older_than_the_seal_horizon_is_dropped_and_a_recent_one_is_kept() {
	// Sealing is by pane time; if it never fired, late rows would silently reopen closed panes.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 10)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 100, 7, 1.0)).build()).expect("apply");
	let kept = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 98, 8, 2.0)).build()).expect("apply");
	h.advance_watermark(DateTime::from_millis(110)).expect("watermark");

	let dropped = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 50, 9, 4.0)).build()).expect("apply");

	assert!(!render(&kept).is_empty(), "a row inside the lateness window is still admitted");
	assert!(render(&dropped).is_empty(), "a row below the horizon must be dropped");
}

#[test]
fn rows_inside_one_pane_share_it_and_do_not_count_toward_capacity() {
	// Bucketing is by pane width; a bucket per row would evict trader 100, the leader, from a capacity of three.
	let mut h = harness!(TopVolume, rolling(30, Some(10), 3_600_000)).expect("harness");

	let out = h
		.apply(TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 3, 100, 9.0))
			.insert(input_row(2, "BTC", 7, 200, 2.0))
			.insert(input_row(3, "BTC", 12, 300, 3.0))
			.insert(input_row(4, "BTC", 25, 400, 1.0))
			.build())
		.expect("apply");

	assert_eq!(render(&out), vec![(DiffType::Insert, 1, 100, 9.0), (DiffType::Insert, 2, 300, 3.0)]);
}

#[test]
fn the_span_ends_at_the_newest_pane_and_reaches_back_one_size() {
	// The guest is handed a span; it must end where the newest pane ends, not at the row time.
	let mut h = harness!(TopVolume, rolling(30, Some(10), 3_600_000)).expect("harness");

	let out =
		h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1_003, 100, 5.0)).build()).expect("apply");

	let row = out.diffs[0].post().expect("post").row_ref(0).expect("row");
	assert_eq!(
		(row.u64("start").expect("start"), row.u64("end").expect("end")),
		(DateTime::from_millis(980).to_order(), DateTime::from_millis(1_010).to_order())
	);
}

#[test]
fn a_ranked_row_is_keyed_by_the_group_bytes_then_the_rank_bytes() {
	// Row numbers already stored under this key layout must stay reachable, so the layout is a compatibility
	// contract.
	let mut h = harness!(TopVolume, rolling(3, Some(1), 3_600_000)).expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 100, 5.0)).build()).expect("apply");

	let mut layout = String::from("BTC").into_encoded_key().as_slice().to_vec();
	layout.extend_from_slice(1u32.into_encoded_key().as_slice());
	let hashed = GroupId::of(&EncodedKey::new(layout));
	let stored_tail: Vec<u8> = hashed.as_bytes()[8..].iter().map(|byte| !byte).collect();

	assert!(
		h.snapshot_state().keys().any(|key| key.as_slice().ends_with(&stored_tail)),
		"no row-number mapping is keyed by the hash of group bytes followed by rank bytes"
	);
}
