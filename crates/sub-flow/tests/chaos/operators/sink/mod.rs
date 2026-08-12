// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The three view sinks, partitioned and not, over inserts, updates and removes.
//!
//! A sink's output is compared off the accumulator rather than off `apply`, because that is the
//! channel consumers actually read. Routing it through the return value instead would strip the
//! object tag those consumers key on.
//!
//! Table and series sinks look close to an identity, which is why the layouts are driven separately:
//! what is under test is that coercion preserves values and that a partitioned write reaches the same
//! rows as an unpartitioned one. The ring buffer is where the oracle earns its keep, and the only
//! kind whose view is a proper subset of what arrived.

pub mod oracle;

use rand::RngExt;
use reifydb_core::{
	interface::{
		catalog::{
			column::{Column, ColumnIndex},
			flow::OperatorId,
			id::{ColumnId, NamespaceId, RingBufferId, SeriesId, TableId, ViewId},
			namespace::Namespace,
			object::ObjectId,
			series::SeriesKey,
			view::{RingBufferView, SeriesView, TableView, View, ViewKind},
		},
		change::{Change, Diff},
		flow::OperatorCapability,
		resolved::{ResolvedNamespace, ResolvedView},
	},
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::{
		Operator,
		sink::{
			ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator,
			view::SinkTableViewOperator,
		},
	},
	transaction::deferred::DeferredTransaction,
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{run_reported, split},
	operator::{
		drive as driver,
		scenario::{BatchSize, Scenario},
		subject::Subject,
	},
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{constraint::TypeConstraint, value_type::ValueType},
};

use crate::{
	framework::harness::Harness,
	operators::{aggregate::workload::AggregateWorkload, sink::oracle::SinkOracle},
};

const SOURCE: OperatorId = OperatorId(0);
const SINK: OperatorId = OperatorId(1);
const VIEW: ViewId = ViewId(42);
const NAMESPACE: NamespaceId = NamespaceId(1);

/// The partition column. The workload must hold it fixed across an update: a partitioned sink
/// refuses one that repartitions, which is a different path from the one under test.
const GROUP_COLUMN: &str = "g";
const VALUE_COLUMN: &str = "v";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
	Table,
	Series,

	/// Bounds a lane, not the sink as a whole.
	Ring {
		capacity: u64,
	},
}

impl Kind {
	/// None where the sink keeps everything it is given.
	fn capacity(self) -> Option<usize> {
		match self {
			Kind::Ring {
				capacity,
			} => Some(capacity as usize),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
	Unpartitioned,
	Partitioned,
}

impl Layout {
	fn partition_by(self) -> Vec<String> {
		match self {
			Layout::Unpartitioned => vec![],
			Layout::Partitioned => vec![GROUP_COLUMN.to_string()],
		}
	}
}

fn column(id: u64, name: &str, ty: ValueType, index: u8) -> Column {
	Column {
		id: ColumnId(id),
		name: name.to_string(),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		index: ColumnIndex(index),
		auto_increment: false,
		dictionary_id: None,
	}
}

// Matches generator::shape so a corpus row coerces without conversion. No dictionary column: those
// need a catalog-backed harness, and are recorded as uncovered rather than faked here.
fn columns() -> Vec<Column> {
	vec![column(1, GROUP_COLUMN, ValueType::Int4, 0), column(2, VALUE_COLUMN, ValueType::Int8, 1)]
}

fn namespace() -> Namespace {
	Namespace::Local {
		id: NAMESPACE,
		name: "chaos".to_string(),
		local_name: "chaos".to_string(),
		parent_id: NamespaceId::ROOT,
	}
}

fn resolved(def: View) -> ResolvedView {
	ResolvedView::new(
		Fragment::internal("sink"),
		ResolvedNamespace::new(Fragment::internal("chaos"), namespace()),
		def,
	)
}

/// United here because `Harness` is generic over a concrete `Operator`.
pub enum SinkOp {
	Table(SinkTableViewOperator),
	Series(SinkSeriesViewOperator),
	Ring(SinkRingBufferViewOperator),
}

impl Operator<DeferredTransaction> for SinkOp {
	fn id(&self) -> OperatorId {
		match self {
			SinkOp::Table(o) => o.id(),
			SinkOp::Series(o) => o.id(),
			SinkOp::Ring(o) => o.id(),
		}
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		match self {
			SinkOp::Table(o) => o.capabilities(),
			SinkOp::Series(o) => o.capabilities(),
			SinkOp::Ring(o) => o.capabilities(),
		}
	}

	fn apply(&mut self, txn: &mut DeferredTransaction, change: Change) -> Result<Change> {
		match self {
			SinkOp::Table(o) => o.apply(txn, change),
			SinkOp::Series(o) => o.apply(txn, change),
			SinkOp::Ring(o) => o.apply(txn, change),
		}
	}

	fn output_schema(&self) -> Option<Columns> {
		match self {
			SinkOp::Table(o) => o.output_schema(),
			SinkOp::Series(o) => o.output_schema(),
			SinkOp::Ring(o) => o.output_schema(),
		}
	}
}

pub fn build(kind: Kind, layout: Layout, _runtime: RuntimeContext) -> SinkOp {
	let partition_by = layout.partition_by();

	match kind {
		Kind::Table => {
			let def = View::Table(TableView {
				id: VIEW,
				namespace: NAMESPACE,
				name: "sink_table".to_string(),
				kind: ViewKind::Deferred,
				columns: columns(),
				primary_key: None,
				storage: TableId(7),
				sort: vec![],
			});
			SinkOp::Table(SinkTableViewOperator::new(SINK, resolved(def), TableId(7), partition_by))
		}
		Kind::Series => {
			let key = SeriesKey::Integer {
				column: VALUE_COLUMN.to_string(),
			};
			let def = View::Series(SeriesView {
				id: VIEW,
				namespace: NAMESPACE,
				name: "sink_series".to_string(),
				kind: ViewKind::Deferred,
				columns: columns(),
				primary_key: None,
				storage: SeriesId(9),
				key: key.clone(),
				tag: None,
				sort: vec![],
			});
			SinkOp::Series(SinkSeriesViewOperator::new(
				SINK,
				resolved(def),
				SeriesId(9),
				key,
				partition_by,
			))
		}
		Kind::Ring {
			capacity,
		} => {
			let def = View::RingBuffer(RingBufferView {
				id: VIEW,
				namespace: NAMESPACE,
				name: "sink_ring".to_string(),
				kind: ViewKind::Deferred,
				columns: columns(),
				primary_key: None,
				storage: RingBufferId(11),
				capacity,
				sort: vec![],
			});
			SinkOp::Ring(SinkRingBufferViewOperator::new(
				SINK,
				resolved(def),
				RingBufferId(11),
				capacity,
				// The only mode whose emissions can be compared at all.
				true,
				None,
				partition_by,
			))
		}
	}
}

/// Turns the accumulator drain into the `Change` the framework expects.
pub struct SinkSubject {
	harness: Harness<SinkOp>,
}

impl Subject for SinkSubject {
	fn apply(&mut self, change: Change) -> Result<Change> {
		let version = change.version;
		let changed_at = change.changed_at;
		let entries = self.harness.apply_emitting(change)?;

		let mut diffs: Vec<Diff> = Vec::with_capacity(entries.len());
		for (object, diff) in entries {
			// Another object's entry would make the comparison below one against a mixture.
			assert_eq!(
				object,
				ObjectId::view(VIEW),
				"a sink emitted a change for an object it does not own"
			);
			diffs.push(diff);
		}
		Ok(Change::from_flow(SINK, version, diffs, changed_at))
	}

	fn tick(&mut self, _at_ms: u64) -> Result<Option<Change>> {
		// No sink seals on the generic timer. Row-TTL eviction needs a timer kind the subject cannot
		// carry, so it stays uncovered and the sweeps keep tick_pct at 0.
		Ok(None)
	}
}

#[derive(Debug, Clone)]
pub struct Params {
	pub kind: Kind,
	pub layout: Layout,
	pub groups: i32,
	pub value_ceiling: i64,
	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let harness = Harness::new(|runtime| build(params.kind, params.layout, runtime));
	let mut subject = SinkSubject {
		harness,
	};
	let workload = AggregateWorkload {
		groups: params.groups,
		value_ceiling: params.value_ceiling,
	};
	let mut model = SinkOracle::new(params.kind.capacity(), params.layout == Layout::Partitioned);

	driver::drive(
		seed,
		Scenario::mixed(params.steps)
			.with_batch(BatchSize::Geometric {
				p: 0.45,
				max: params.max_batch,
			})
			.with_mix(params.remove_pct, params.update_pct, 0)
			.with_max_live(params.max_live),
		&mut subject,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let max_batch = rng.random_range(1..=8u32);
	let max_live = rng.random_range(8..=40usize);
	let kind = match rng.random_range(0..3u32) {
		0 => Kind::Table,
		1 => Kind::Series,
		// Drawn against max_live so both the evicting and non-evicting regimes are reached.
		_ => Kind::Ring {
			capacity: rng.random_range(1..=(max_live as u64 + 4)),
		},
	};
	let layout = match rng.random_range(0..2u32) {
		0 => Layout::Unpartitioned,
		_ => Layout::Partitioned,
	};
	let params = Params {
		kind,
		layout,
		groups: rng.random_range(1..=5i32),
		value_ceiling: rng.random_range(4..=100i64),
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live,
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(20..=50u32),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("sink_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}

/// One per cell, so a regression names the cell it broke.
pub fn params(kind: Kind, layout: Layout) -> Params {
	Params {
		kind,
		layout,
		groups: 3,
		value_ceiling: 40,
		steps: 60,
		max_batch: 5,
		max_live: 24,
		remove_pct: 20,
		update_pct: 35,
	}
}
