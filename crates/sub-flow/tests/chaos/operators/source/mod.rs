// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The four source operators over inserts, updates and removes.
//!
//! Their `apply` bodies are byte-identical, so the matrix is not four contracts but a guard against
//! them drifting apart unnoticed. What is actually under test is dictionary decode, the only thing a
//! source does: a plain column passes through untouched and a sweep over one would prove nothing.
//!
//! The retracted half is NOT covered here and cannot be: a folded view keys an update's `pre` by row
//! number and never reads its values, so a decode that skips `pre` passes every iteration. That half
//! is held by `regression/update_pre_fidelity.rs`, which reads the diffs directly.

pub mod oracle;
pub mod workload;

use rand::RngExt;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	common::TimeSource,
	interface::{
		catalog::{
			flow::OperatorId,
			id::{NamespaceId, RingBufferId, SeriesId, TableId, ViewId},
			ringbuffer::RingBuffer,
			series::SeriesKey,
			table::Table,
			view::{SeriesView, View, ViewKind},
		},
		change::Change,
	},
	value::column::columns::Columns,
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_sub_flow::operator::scan::{
	ringbuffer::SourceRingBufferOperator, series::SourceSeriesOperator, table::SourceTableOperator,
	view::SourceViewOperator,
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{run_reported, split},
	operator::{
		drive as driver,
		scenario::{BatchSize, Scenario},
	},
};
use reifydb_value::Result;

use crate::{
	framework::harness::Harness,
	operators::source::{
		oracle::SourceOracle,
		workload::{SourceWorkload, VALUE_COLUMN},
	},
};

pub const SOURCE: OperatorId = OperatorId(0);

const NAMESPACE: &str = "chaos";
const DICTIONARY: &str = "syms";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
	Series,
	Table,
	View,
	RingBuffer,
}

pub const MATRIX: [Kind; 4] = [Kind::Series, Kind::Table, Kind::View, Kind::RingBuffer];

/// United here because `Harness` is generic over a concrete `Operator`.
pub enum SourceOp {
	Series(SourceSeriesOperator),
	Table(SourceTableOperator),
	View(SourceViewOperator),
	RingBuffer(SourceRingBufferOperator),
}

impl Operator for SourceOp {
	fn id(&self) -> OperatorId {
		match self {
			SourceOp::Series(o) => o.id(),
			SourceOp::Table(o) => o.id(),
			SourceOp::View(o) => o.id(),
			SourceOp::RingBuffer(o) => o.id(),
		}
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		match self {
			SourceOp::Series(o) => o.capabilities(),
			SourceOp::Table(o) => o.capabilities(),
			SourceOp::View(o) => o.capabilities(),
			SourceOp::RingBuffer(o) => o.capabilities(),
		}
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		match self {
			SourceOp::Series(o) => o.apply(txn, change),
			SourceOp::Table(o) => o.apply(txn, change),
			SourceOp::View(o) => o.apply(txn, change),
			SourceOp::RingBuffer(o) => o.apply(txn, change),
		}
	}

	fn output_schema(&self) -> Option<Columns> {
		match self {
			SourceOp::Series(o) => Operator::output_schema(o),
			SourceOp::Table(o) => Operator::output_schema(o),
			SourceOp::View(o) => Operator::output_schema(o),
			SourceOp::RingBuffer(o) => Operator::output_schema(o),
		}
	}
}

// The defs only have to name the two columns; a source reads its column list for `output_schema`
// and never consults it while decoding.
fn series_view() -> View {
	View::Series(SeriesView {
		id: ViewId(42),
		namespace: NamespaceId(1),
		name: "src".to_string(),
		kind: ViewKind::Deferred,
		columns: vec![],
		primary_key: None,
		storage: SeriesId(9),
		key: SeriesKey::Integer {
			column: VALUE_COLUMN.to_string(),
		},
		tag: None,
		sort: vec![],
	})
}

fn table() -> Table {
	Table {
		id: TableId(7),
		namespace: NamespaceId(1),
		name: "src".to_string(),
		columns: vec![],
		primary_key: None,
		partition_by: vec![],
		underlying: false,
		time: TimeSource::Processing,
	}
}

fn ringbuffer() -> RingBuffer {
	RingBuffer {
		id: RingBufferId(11),
		namespace: NamespaceId(1),
		name: "src".to_string(),
		columns: vec![],
		primary_key: None,
		capacity: 64,
		partition_by: vec![],
		underlying: false,
		time: TimeSource::Processing,
	}
}

pub fn build(kind: Kind) -> SourceOp {
	match kind {
		Kind::Series => SourceOp::Series(SourceSeriesOperator::new(SOURCE)),
		Kind::Table => SourceOp::Table(SourceTableOperator::new(SOURCE, table())),
		Kind::View => SourceOp::View(SourceViewOperator::new(SOURCE, series_view())),
		Kind::RingBuffer => SourceOp::RingBuffer(SourceRingBufferOperator::new(SOURCE, ringbuffer())),
	}
}

fn declare(engine: &TestEngine) {
	engine.admin(&format!("CREATE NAMESPACE {NAMESPACE}"));
	engine.admin(&format!("CREATE DICTIONARY {NAMESPACE}::{DICTIONARY} FOR utf8 AS uint2"));
}

#[derive(Debug, Clone)]
pub struct Params {
	pub kind: Kind,
	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::with_engine(|engine, _| {
		declare(engine);
		build(params.kind)
	})
	.with_dictionaries();

	let catalog = harness.engine().inner().catalog().clone();
	let namespace = catalog.cache().find_namespace_by_name(NAMESPACE).expect("the namespace was declared");
	let dictionary = catalog
		.cache()
		.find_dictionary_by_name(namespace.id(), DICTIONARY)
		.expect("the dictionary was declared");

	let workload = SourceWorkload {
		dictionary,
		registry: harness.dictionary_registry(),
		interned: Default::default(),
	};
	let mut model = SourceOracle::new();

	driver::drive(
		seed,
		Scenario::mixed(params.steps)
			.with_batch(BatchSize::Geometric {
				p: 0.45,
				max: params.max_batch,
			})
			.with_mix(params.remove_pct, params.update_pct, 0)
			.with_max_live(params.max_live),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

pub fn params(kind: Kind) -> Params {
	Params {
		kind,
		steps: 60,
		max_batch: 5,
		max_live: 24,
		remove_pct: 20,
		update_pct: 35,
	}
}

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let max_batch = rng.random_range(1..=8u32);
	let params = Params {
		kind: MATRIX[rng.random_range(0..MATRIX.len())],
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=40usize),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(20..=50u32),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("source_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
