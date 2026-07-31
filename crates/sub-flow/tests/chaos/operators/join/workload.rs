// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The join family's corpus: a row belongs to one of two inputs, and a change carries both.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_testing_chaos::operator::workload::{Lanes, Op, Workload};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType,
	},
};

pub const LEFT_NODE: FlowNodeId = FlowNodeId(10);
pub const RIGHT_NODE: FlowNodeId = FlowNodeId(11);
pub const JOIN_NODE: FlowNodeId = FlowNodeId(12);

pub const LEFT_COLUMNS: [(&str, ValueType); 3] =
	[("lid", ValueType::Int8), ("k", ValueType::Int4), ("lv", ValueType::Int8)];

pub const RIGHT_COLUMNS: [(&str, ValueType); 3] =
	[("rid", ValueType::Int8), ("k", ValueType::Int4), ("rv", ValueType::Int8)];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Side {
	Left,
	Right,
}

impl Side {
	fn node(self) -> FlowNodeId {
		match self {
			Side::Left => LEFT_NODE,
			Side::Right => RIGHT_NODE,
		}
	}

	fn spec(self) -> &'static [(&'static str, ValueType); 3] {
		match self {
			Side::Left => &LEFT_COLUMNS,
			Side::Right => &RIGHT_COLUMNS,
		}
	}
}

#[derive(Clone, Debug)]
pub struct JoinRow {
	pub side: Side,
	pub number: RowNumber,

	/// `None` is the join's undefined key, which each strategy treats differently: an inner join
	/// drops the row entirely, a left join publishes it as permanently unmatched.
	pub key: Option<i32>,

	pub value: i64,

	/// The row's event position, which is what the sweep ages this row's side group on.
	///
	/// Without it every row stamps at `DateTime::default()`, so a join's whole corpus lands in one
	/// activity bucket and any cutoff is all-or-nothing: either nothing is ever due, or the first
	/// sweep past the epoch takes the entire node. Either way a keyspace or mapping assertion holds
	/// for a reason that has nothing to do with the phase it names.
	pub coord_ms: u64,
}

impl JoinRow {
	pub fn at(&self) -> DateTime {
		DateTime::from_timestamp_millis(self.coord_ms).expect("a corpus coordinate is representable")
	}
}

/// A zero-row `Columns` naming one side's shape. The join needs the right one at construction time
/// to know which columns an unmatched left row fills with none, and their types decide which
/// `Value::None` variant it fills them with.
pub fn schema(spec: &[(&str, ValueType)]) -> Columns {
	Columns::new(
		spec.iter()
			.map(|(name, ty)| {
				ColumnWithName::new(
					Fragment::internal(*name),
					ColumnBuffer::with_capacity(ty.clone(), 0),
				)
			})
			.collect(),
	)
}

fn columns_of(rows: &[&JoinRow]) -> Columns {
	let spec = rows[0].side.spec();
	let mut buffers: Vec<ColumnBuffer> =
		spec.iter().map(|(_, ty)| ColumnBuffer::with_capacity(ty.clone(), rows.len())).collect();
	for row in rows {
		buffers[0].push_value(Value::Int8(row.number.0 as i64));
		buffers[1].push_value(match row.key {
			Some(key) => Value::Int4(key),
			None => Value::none(),
		});
		buffers[2].push_value(Value::Int8(row.value));
	}
	let columns = spec
		.iter()
		.zip(buffers)
		.map(|((name, _), buffer)| ColumnWithName::new(Fragment::internal(*name), buffer))
		.collect();

	// `with_row_numbers` would fill every system stamp with `DateTime::default()`, which is the epoch
	// and not a position any of these rows carries. The times have to be written alongside the row
	// numbers or the harness reads the whole batch as arriving at time zero.
	let numbers: Vec<RowNumber> = rows.iter().map(|row| row.number).collect();
	let times: Vec<DateTime> = rows.iter().map(|row| row.at()).collect();
	Columns::with_system(columns, SystemColumns::new(numbers, Vec::new(), times.clone(), times.clone(), times))
}

fn tagged(mut diff: Diff, side: Side) -> Diff {
	// The join reads the side off each diff, not off the change, which is what lets one change carry
	// both inputs the way a real flow batch does.
	diff.set_origin(Some(ChangeOrigin::Flow(side.node())));
	diff
}

fn change(diffs: Vec<Diff>) -> Change {
	// The parent origin is only ever a fallback here since every diff names its own, but it must not
	// be the join's own node - the operator short-circuits a change it published itself.
	Change::from_flow(LEFT_NODE, CommitVersion(1), diffs, DateTime::default())
}

pub struct JoinWorkload {
	pub keys: i32,
	pub right_pct: u32,
	pub none_pct: u32,
	pub rekey_pct: u32,

	/// The span rows draw their event position from. A join has no window, so this is not a shape
	/// parameter - it only decides how widely the corpus spreads across the activity grid, which is
	/// what decides whether a sweep can retire one side group while another stays live.
	pub coord_span_ms: u64,

	/// Whether an update may move a key between defined and undefined. Off everywhere but the sweep
	/// written for that transition, because it is routed to a handler that leaves the operator's own
	/// state behind and would otherwise fail every sweep for the same single reason.
	pub flip_definedness: bool,
}

impl JoinWorkload {
	fn draw_key(&self, rng: &mut StdRng) -> Option<i32> {
		let defined = rng.random_range(1..=self.keys);
		match rng.random_range(0..100u32) < self.none_pct {
			true => None,
			false => Some(defined),
		}
	}
}

impl Workload for JoinWorkload {
	type Row = JoinRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> JoinRow {
		let side = match rng.random_range(0..100u32) < self.right_pct {
			true => Side::Right,
			false => Side::Left,
		};
		JoinRow {
			side,
			number,
			key: self.draw_key(rng),
			coord_ms: rng.random_range(0..self.coord_span_ms),
			value: rng.random_range(1..100i64),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &JoinRow) -> JoinRow {
		// Every draw happens whatever the row holds, so what a revalue costs the stream does not
		// depend on the row it is applied to and a pinned corpus stays reproducible.
		let defined = rng.random_range(1..=self.keys);
		let undefined = rng.random_range(0..100u32) < self.none_pct;
		let rekey = rng.random_range(0..100u32) < self.rekey_pct;
		let value = rng.random_range(1..100i64);

		let key = match (rekey, self.flip_definedness) {
			(false, _) => row.key,
			(true, true) => (!undefined).then_some(defined),
			// Definedness preserved: a defined key moves to another defined key, an undefined
			// one stays undefined.
			(true, false) => row.key.map(|_| defined),
		};

		JoinRow {
			key,
			value,
			..row.clone()
		}
	}

	fn lanes(&self, row: &JoinRow) -> Lanes {
		// The coord lane is what the driver folds into its arrival frontier, and the watermark it
		// sweeps at is clamped to that frontier. This lane used to carry the side, which pinned every
		// join corpus to an arrival of 1ms and made any reclaiming scenario unable to advance past the
		// epoch. The side is not lost from the fingerprint: it is drawn once per row alongside the
		// number, so it is a function of the number within a run.
		Lanes {
			number: row.number.0,
			group: row.key.map(|key| key as u64).unwrap_or(u64::MAX),
			coord: row.coord_ms,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[JoinRow]) -> Change {
		change(coalesce(&rows.iter().cloned().map(Op::Insert).collect::<Vec<_>>()))
	}

	fn remove(&self, row: &JoinRow) -> Change {
		change(vec![tagged(Diff::remove(columns_of(&[row])), row.side)])
	}

	fn update(&self, pre: &JoinRow, post: &JoinRow) -> Change {
		change(vec![tagged(Diff::update(columns_of(&[pre]), columns_of(&[post])), pre.side)])
	}

	fn change(&self, ops: &[Op<JoinRow>]) -> Change {
		change(coalesce(ops))
	}

	fn projection(&self) -> &[usize] {
		&[]
	}
}

/// Packs a run of operations into diffs the way an upstream flow would: consecutive operations of
/// the same kind on the same input become one multi-row diff.
///
/// Coalescing only across a RUN rather than over the whole batch is what keeps the relative order of
/// the two inputs intact. Latest mode is order-sensitive - a right arrival overwrites a per-key slot
/// - so reordering the inputs against each other would make the oracle describe a sequence the
/// operator never saw. Within a run there is nothing to reorder, and the multi-row diff is what
/// reaches the batched paths: keyed grouping, batched unmatched-left, multi-index cartesian.
fn coalesce(ops: &[Op<JoinRow>]) -> Vec<Diff> {
	let mut diffs = Vec::new();
	let mut start = 0;
	while start < ops.len() {
		let mut end = start + 1;
		while end < ops.len() && continues_run(&ops[start..end], &ops[end]) {
			end += 1;
		}
		diffs.push(diff_of(&ops[start..end]));
		start = end;
	}
	diffs
}

fn continues_run(run: &[Op<JoinRow>], next: &Op<JoinRow>) -> bool {
	let same_shape = side_of(&run[0]) == side_of(next)
		&& matches!(
			(&run[0], next),
			(Op::Insert(_), Op::Insert(_))
				| (Op::Remove(_), Op::Remove(_))
				| (Op::Update(..), Op::Update(..))
		);
	// One diff never names the same row twice. A source that writes a row twice in a batch folds
	// those into a single operation before it emits anything, so a diff carrying both would be a
	// shape no operator is built for - here both copies would key the same (left, right) mapping and
	// the pair would be published twice under one output row.
	same_shape && !run.iter().any(|op| identity_of(op) == identity_of(next))
}

fn side_of(op: &Op<JoinRow>) -> Side {
	match op {
		Op::Insert(row) | Op::Remove(row) | Op::Update(row, _) => row.side,
	}
}

fn identity_of(op: &Op<JoinRow>) -> (Side, u64) {
	match op {
		Op::Insert(row) | Op::Remove(row) | Op::Update(row, _) => (row.side, row.number.0),
	}
}

fn diff_of(run: &[Op<JoinRow>]) -> Diff {
	let side = side_of(&run[0]);
	match &run[0] {
		Op::Insert(_) => {
			let rows: Vec<&JoinRow> = run
				.iter()
				.map(|op| match op {
					Op::Insert(row) => row,
					_ => unreachable!("a run holds one kind"),
				})
				.collect();
			tagged(Diff::insert(columns_of(&rows)), side)
		}
		Op::Remove(_) => {
			let rows: Vec<&JoinRow> = run
				.iter()
				.map(|op| match op {
					Op::Remove(row) => row,
					_ => unreachable!("a run holds one kind"),
				})
				.collect();
			tagged(Diff::remove(columns_of(&rows)), side)
		}
		Op::Update(..) => {
			let pairs: Vec<(&JoinRow, &JoinRow)> = run
				.iter()
				.map(|op| match op {
					Op::Update(pre, post) => (pre, post),
					_ => unreachable!("a run holds one kind"),
				})
				.collect();
			let pre: Vec<&JoinRow> = pairs.iter().map(|(pre, _)| *pre).collect();
			let post: Vec<&JoinRow> = pairs.iter().map(|(_, post)| *post).collect();
			tagged(Diff::update(columns_of(&pre), columns_of(&post)), side)
		}
	}
}
