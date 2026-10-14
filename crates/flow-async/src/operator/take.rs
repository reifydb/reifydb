// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use reifydb_codec::row::{
	bytes::EncodedBytes,
	envelope::{Envelope, EnvelopeBuilder},
	operator::state::{decode, encode},
	pod::{EncodedPodRow, EncodedPodRowBuilder},
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	key::{
		operator::{
			keyspace::root::{CustomUnmanaged, CustomUnmanagedSuffix},
			state::{GroupId, GroupStateKey},
		},
		typed::direction::Asc,
	},
	metrics::heap::HeapSize,
	state::typed::typed_key,
	value::batch::{from_encoded_bytes, take_rows},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	util::cowvec::CowVec,
	value::{
		column_view::ColumnView,
		container::temporal_array::datetime_array,
		datetime::DateTime,
		row_number::RowNumber,
		system_columns::{
			SystemColumn, created_at, require_row_numbers, stamp_system_columns, updated_at, user_columns,
		},
	},
};
use tracing::instrument;

use crate::{
	error::FlowStateError,
	operator::{HostOperator, host::HostContext, row_times, state::store},
};

#[operator_state]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, HeapSize)]
struct RowAge {
	created_at: DateTime,
	row: RowNumber,
}

impl RowAge {
	fn of(created_at: Option<DateTime>, row: RowNumber) -> Self {
		reifydb_assertions! {
			assert!(
				created_at.is_some(),
				"row {:?} reached take without a created_at, so it ties with every other stampless row at \
				 the epoch and the window silently orders by row number alone",
				row
			);
		}
		Self {
			created_at: created_at.unwrap_or_default(),
			row,
		}
	}
}

#[operator_state]
#[derive(Debug, Clone, Default, HeapSize)]
struct TakeState {
	by_age: BTreeMap<RowAge, RowNumber>,
	by_row: HashMap<RowNumber, (RowAge, usize)>,
	candidates_by_age: BTreeMap<RowAge, RowNumber>,
	candidates_by_row: HashMap<RowNumber, (RowAge, usize)>,
	row_data: HashMap<RowNumber, EncodedBytes>,
}

pub struct TakePlan {
	parent_schema: Option<SchemaRef>,
	operator: OperatorId,
	limit: usize,
}

pub struct TakeOperator {
	plan: TakePlan,
	shape: Option<(SchemaRef, RowShape)>,
}

fn user_views(columns: &RecordBatch) -> Result<Vec<ColumnView<'_>>> {
	user_columns(columns).map(|(field, array)| ColumnView::try_from((array, field.as_ref()))).collect()
}

fn row_shape_from_columns(cols: &RecordBatch) -> Result<RowShape> {
	let fields: Vec<RowShapeField> = user_views(cols)?
		.iter()
		.map(|view| RowShapeField::unconstrained(view.field.name().clone(), view.get_type()))
		.collect();
	Ok(RowShape::new(RowFamily::Pod, fields))
}

fn row_shape_for(cache: &mut Option<(SchemaRef, RowShape)>, cols: &RecordBatch) -> Result<RowShape> {
	if let Some((schema, shape)) = cache.as_ref()
		&& schema == cols.schema_ref()
	{
		return Ok(shape.clone());
	}
	let shape = row_shape_from_columns(cols)?;
	*cache = Some((cols.schema(), shape.clone()));
	Ok(shape)
}

fn encode_take_rows(shape: &RowShape, columns: &RecordBatch) -> Result<Vec<EncodedBytes>> {
	let views = user_views(columns)?;
	let mut bodies: Vec<EncodedPodRowBuilder> = (0..columns.num_rows()).map(|_| shape.allocate_pod()).collect();
	shape.write_columns(&mut bodies, &views)?;
	let created = created_at(columns)?;
	let updated = updated_at(columns)?;
	let times = row_times(columns)?;
	Ok(bodies
		.into_iter()
		.enumerate()
		.map(|(row, body)| {
			let mut envelope = EnvelopeBuilder::new()
				.created_at(created.get(row).copied().unwrap_or_default())
				.updated_at(updated.get(row).copied().unwrap_or_default());
			if let Some(time) = times.get(row).copied().flatten() {
				envelope = envelope.time(time);
			}
			envelope.build(body.freeze().as_slice()).into_bytes()
		})
		.collect())
}

fn decode_take_rows(shape: &RowShape, rows: &[(RowNumber, EncodedBytes)]) -> Result<RecordBatch> {
	let mut envelopes: Vec<&Envelope> = Vec::with_capacity(rows.len());
	for (_, encoded) in rows {
		envelopes.push(Envelope::try_view(EncodedPodRow::view(encoded))?);
	}
	let bodies: Vec<EncodedBytes> =
		envelopes.iter().map(|envelope| EncodedBytes(CowVec::new(envelope.body().to_vec()))).collect();
	let numbers: Vec<RowNumber> = rows.iter().map(|&(row_number, _)| row_number).collect();

	let decoded = from_encoded_bytes(shape, &numbers, &bodies)?;
	let mut stamps: Vec<(SystemColumn, ArrayRef)> = vec![
		(
			SystemColumn::CreatedAt,
			Arc::new(datetime_array(
				envelopes.iter().map(|envelope| envelope.created_at().unwrap_or_default()),
			)),
		),
		(
			SystemColumn::UpdatedAt,
			Arc::new(datetime_array(
				envelopes.iter().map(|envelope| envelope.updated_at().unwrap_or_default()),
			)),
		),
	];
	if let Some(times) = envelopes.iter().map(|envelope| envelope.time()).collect::<Option<Vec<DateTime>>>() {
		stamps.push((SystemColumn::Time, Arc::new(datetime_array(times))));
	}
	stamp_system_columns(decoded, stamps)
}

fn is_timed(encoded: &EncodedBytes) -> Result<bool> {
	Ok(Envelope::try_view(EncodedPodRow::view(encoded))?.time().is_some())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
	Insert,
	Update,
	Remove,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
	Input,
	Stored {
		timed: bool,
	},
}

struct Group {
	kind: Kind,
	source: Source,
	indices: Vec<usize>,
	stored: Vec<(RowNumber, EncodedBytes)>,
}

struct Output<'a> {
	pre: &'a RecordBatch,
	post: &'a RecordBatch,
	epochs: Vec<Vec<Group>>,
	seen: HashSet<RowNumber>,
}

impl<'a> Output<'a> {
	fn new(pre: &'a RecordBatch, post: &'a RecordBatch) -> Self {
		Self {
			pre,
			post,
			epochs: vec![Vec::new()],
			seen: HashSet::new(),
		}
	}

	fn input(&mut self, kind: Kind, row_number: RowNumber, row_idx: usize) {
		self.group(kind, Source::Input, row_number).indices.push(row_idx);
	}

	fn stored(&mut self, kind: Kind, row_number: RowNumber, encoded: &EncodedBytes) -> Result<()> {
		let source = Source::Stored {
			timed: is_timed(encoded)?,
		};
		self.group(kind, source, row_number).stored.push((row_number, encoded.clone()));
		Ok(())
	}

	fn group(&mut self, kind: Kind, source: Source, row_number: RowNumber) -> &mut Group {
		if !self.seen.insert(row_number) {
			self.seen.clear();
			self.seen.insert(row_number);
			self.epochs.push(Vec::new());
		}
		let epoch = self.epochs.last_mut().unwrap();
		match epoch.iter().position(|group| group.kind == kind && group.source == source) {
			Some(position) => &mut epoch[position],
			None => {
				epoch.push(Group {
					kind,
					source,
					indices: Vec::new(),
					stored: Vec::new(),
				});
				epoch.last_mut().unwrap()
			}
		}
	}

	fn finish(self, shape: &RowShape, output_diffs: &mut Vec<Diff>) -> Result<()> {
		for group in self.epochs.into_iter().flatten() {
			match (group.kind, group.source) {
				(Kind::Insert, Source::Input) => {
					output_diffs.push(Diff::insert(take_rows(self.post, &group.indices)?))
				}
				(Kind::Remove, Source::Input) => {
					output_diffs.push(Diff::remove(take_rows(self.pre, &group.indices)?))
				}
				(Kind::Update, _) => output_diffs.push(Diff::update(
					take_rows(self.pre, &group.indices)?,
					take_rows(self.post, &group.indices)?,
				)),
				(
					kind,
					Source::Stored {
						..
					},
				) => {
					let cols = decode_take_rows(shape, &group.stored)?;
					if user_columns(&cols).next().is_some() {
						output_diffs.push(match kind {
							Kind::Insert => Diff::insert(cols),
							_ => Diff::remove(cols),
						});
					}
				}
			}
		}
		Ok(())
	}
}

impl TakeOperator {
	pub fn new(parent_schema: Option<SchemaRef>, operator: OperatorId, limit: usize) -> Self {
		Self {
			plan: TakePlan {
				parent_schema,
				operator,
				limit,
			},
			shape: None,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<SchemaRef> {
		self.plan.parent_schema.clone()
	}
}

impl TakePlan {
	fn state_key() -> GroupStateKey {
		typed_key::<CustomUnmanaged>(
			GroupId::ROOT,
			&CustomUnmanagedSuffix {
				id: Asc([0u8; 16]),
			},
		)
	}

	fn load_take_state(&self, host: &mut dyn HostContext) -> Result<TakeState> {
		let key = Self::state_key();
		let Some(row) = store::state_get(host, &key)? else {
			return Ok(TakeState::default());
		};
		if row.is_empty() {
			return Ok(TakeState::default());
		}
		decode::<TakeState>(&row).map_err(|e| {
			Error::from(FlowStateError::Decode {
				state: "TakeState",
				cause: e.to_string(),
			})
		})
	}

	fn store_take_state(&self, host: &mut dyn HostContext, state: &TakeState) -> Result<()> {
		let row = encode(state).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "TakeState",
				cause: e.to_string(),
			})
		})?;
		store::state_set(host, &Self::state_key(), row)
	}

	#[inline]
	fn prune_candidates(&self, state: &mut TakeState) {
		let cap = self.limit.saturating_mul(4);
		while state.candidates_by_age.len() > cap {
			let Some((&oldest_age, &oldest_row)) = state.candidates_by_age.iter().next() else {
				break;
			};
			state.candidates_by_age.remove(&oldest_age);
			state.candidates_by_row.remove(&oldest_row);
			state.row_data.remove(&oldest_row);
		}
	}

	#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
	fn assert_candidates_stay_older_than_live(&self, state: &TakeState) {
		reifydb_assertions! {
			if let (Some(newest_candidate), Some(oldest_live)) =
				(state.candidates_by_age.keys().next_back(), state.by_age.keys().next())
			{
				assert!(
					newest_candidate < oldest_live,
					"a demoted candidate is at least as new as the oldest live row, so the next removal \
					 promotes a row the subscriber should already hold and skips the one that should \
					 enter (candidate={:?}, live={:?})",
					newest_candidate,
					oldest_live
				);
			}
		}
	}

	#[inline]
	fn promote_one_candidate(&self, state: &mut TakeState, output: &mut Output) -> Result<()> {
		let Some((&age, &row_number)) = state.candidates_by_age.iter().next_back() else {
			return Ok(());
		};
		let count = state.candidates_by_row.get(&row_number).map(|(_, c)| *c).unwrap_or(1);
		state.candidates_by_age.remove(&age);
		state.candidates_by_row.remove(&row_number);
		state.by_age.insert(age, row_number);
		state.by_row.insert(row_number, (age, count));

		if let Some(encoded) = state.row_data.get(&row_number) {
			output.stored(Kind::Insert, row_number, encoded)?;
		}
		Ok(())
	}

	#[inline]
	fn admit_new_row(
		&self,
		state: &mut TakeState,
		row_number: RowNumber,
		created_at: Option<DateTime>,
		(row_idx, encoded): (usize, EncodedBytes),
		output: &mut Output,
	) -> Result<()> {
		if self.limit == 0 {
			return Ok(());
		}

		let age = RowAge::of(created_at, row_number);
		state.row_data.insert(row_number, encoded);

		if state.by_age.len() >= self.limit
			&& state.by_age.keys().next().is_some_and(|oldest_live| age <= *oldest_live)
		{
			state.candidates_by_age.insert(age, row_number);
			state.candidates_by_row.insert(row_number, (age, 1));
			self.prune_candidates(state);
			return Ok(());
		}

		state.by_age.insert(age, row_number);
		state.by_row.insert(row_number, (age, 1));
		output.input(Kind::Insert, row_number, row_idx);

		if state.by_age.len() > self.limit {
			let oldest = state.by_age.iter().next().map(|(a, r)| (*a, *r));
			if let Some((oldest_age, oldest_row)) = oldest {
				let count = state.by_row.get(&oldest_row).map(|(_, c)| *c).unwrap_or(1);
				state.by_age.remove(&oldest_age);
				state.by_row.remove(&oldest_row);
				state.candidates_by_age.insert(oldest_age, oldest_row);
				state.candidates_by_row.insert(oldest_row, (oldest_age, count));
				if let Some(encoded) = state.row_data.get(&oldest_row) {
					output.stored(Kind::Remove, oldest_row, encoded)?;
				}
			}
		}

		self.prune_candidates(state);
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::take::insert", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_insert_diff(
		&self,
		state: &mut TakeState,
		post: RecordBatch,
		shapes: &mut Option<(SchemaRef, RowShape)>,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let row_count = post.num_rows();
		if row_count == 0 {
			return Ok(());
		}
		let schema = row_shape_for(shapes, &post)?;
		let row_numbers = require_row_numbers(&post)?;
		let created = created_at(&post)?;
		let encoded = encode_take_rows(&schema, &post)?;
		let mut output = Output::new(&post, &post);
		for (row_idx, &row_number) in row_numbers.iter().enumerate().take(row_count) {
			if let Some(slot) = state.by_row.get_mut(&row_number) {
				slot.1 += 1;
				continue;
			}

			if let Some(slot) = state.candidates_by_row.get_mut(&row_number) {
				slot.1 += 1;
				continue;
			}

			self.admit_new_row(
				state,
				row_number,
				created.get(row_idx).copied(),
				(row_idx, encoded[row_idx].clone()),
				&mut output,
			)?;
		}
		output.finish(&schema, output_diffs)
	}

	#[inline]
	#[instrument(name = "flow::operator::take::update", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_update_diff(
		&self,
		state: &mut TakeState,
		pre: RecordBatch,
		post: RecordBatch,
		shapes: &mut Option<(SchemaRef, RowShape)>,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let row_count = post.num_rows();
		if row_count == 0 {
			return Ok(());
		}
		let schema = row_shape_for(shapes, &post)?;
		let row_numbers = require_row_numbers(&post)?;
		let created = created_at(&post)?;
		let encoded = encode_take_rows(&schema, &post)?;
		let mut output = Output::new(&pre, &post);
		let mut updated: Vec<(RowNumber, usize)> = Vec::new();

		for (row_idx, &row_number) in row_numbers.iter().enumerate().take(row_count) {
			if state.by_row.contains_key(&row_number) {
				updated.push((row_number, row_idx));
				state.row_data.insert(row_number, encoded[row_idx].clone());
				continue;
			}

			if state.candidates_by_row.contains_key(&row_number) {
				state.row_data.insert(row_number, encoded[row_idx].clone());
				continue;
			}

			self.admit_new_row(
				state,
				row_number,
				created.get(row_idx).copied(),
				(row_idx, encoded[row_idx].clone()),
				&mut output,
			)?;
		}

		for (row_number, row_idx) in updated {
			output.input(Kind::Update, row_number, row_idx);
		}
		output.finish(&schema, output_diffs)
	}

	#[inline]
	#[instrument(name = "flow::operator::take::remove", level = "trace", skip_all, fields(rows = pre.num_rows()))]
	fn apply_remove_diff(
		&self,
		state: &mut TakeState,
		pre: RecordBatch,
		shapes: &mut Option<(SchemaRef, RowShape)>,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let row_count = pre.num_rows();
		if row_count == 0 {
			return Ok(());
		}
		let schema = row_shape_for(shapes, &pre)?;
		let row_numbers = require_row_numbers(&pre)?;
		let mut output = Output::new(&pre, &pre);
		for (row_idx, &row_number) in row_numbers.iter().enumerate().take(row_count) {
			if let Some(slot) = state.by_row.get_mut(&row_number) {
				if slot.1 > 1 {
					slot.1 -= 1;
					continue;
				}
				let age = slot.0;
				state.by_row.remove(&row_number);
				state.by_age.remove(&age);
				state.row_data.remove(&row_number);
				output.input(Kind::Remove, row_number, row_idx);

				if state.by_age.len() < self.limit && !state.candidates_by_age.is_empty() {
					self.promote_one_candidate(state, &mut output)?;
				}
				continue;
			}

			if let Some(slot) = state.candidates_by_row.get_mut(&row_number) {
				if slot.1 > 1 {
					slot.1 -= 1;
				} else {
					let age = slot.0;
					state.candidates_by_row.remove(&row_number);
					state.candidates_by_age.remove(&age);
					state.row_data.remove(&row_number);
				}
			}
		}
		output.finish(&schema, output_diffs)
	}
}

impl HostOperator for TakeOperator {
	fn id(&self) -> OperatorId {
		self.plan.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let mut state = self.plan.load_take_state(host)?;

		let mut output_diffs = Vec::new();
		let version = change.version;

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.plan.apply_insert_diff(
					&mut state,
					post,
					&mut self.shape,
					&mut output_diffs,
				)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.plan.apply_update_diff(
					&mut state,
					pre,
					post,
					&mut self.shape,
					&mut output_diffs,
				)?,
				Diff::Remove {
					pre,
					..
				} => self.plan.apply_remove_diff(&mut state, pre, &mut self.shape, &mut output_diffs)?,
			}
		}

		self.plan.assert_candidates_stay_older_than_live(&state);
		self.plan.store_take_state(host, &state)?;

		Ok(Change::from_flow(self.plan.operator, version, output_diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.output_schema()
	}
}

#[cfg(test)]
mod tests {
	use arrow_array::UInt64Array;
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion},
		value::{batch::batch, column::factory::int4},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::value::{
		Value,
		system_columns::{row_numbers, system_column, time, with_system_column},
	};

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		transaction::{deferred::DeferredTransaction, mock::FlowTxn},
	};

	fn stamped(n: i32, rn: u64, stamps: Vec<(SystemColumn, DateTime)>) -> RecordBatch {
		let user = batch(vec![int4("n", [n])]).unwrap();
		let with_row =
			with_system_column(user, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![rn])))
				.unwrap();
		stamps.into_iter().fold(with_row, |cols, (column, at)| {
			with_system_column(cols, column, Arc::new(datetime_array([at]))).unwrap()
		})
	}

	fn row(n: i32, rn: u64, born_nanos: i64) -> RecordBatch {
		// created_at is the age the operator must sort on, so every fixture sets it apart from arrival order.
		let at = DateTime::from_nanos(born_nanos);
		stamped(
			n,
			rn,
			vec![(SystemColumn::CreatedAt, at), (SystemColumn::UpdatedAt, at), (SystemColumn::Time, at)],
		)
	}

	fn stamped_row(rn: u64, created: i64, updated: i64, time: Option<i64>) -> RecordBatch {
		// the round trip is only lossless if each stamp lands in its own slot, so every fixture value differs.
		let mut stamps = vec![
			(SystemColumn::CreatedAt, DateTime::from_nanos(created)),
			(SystemColumn::UpdatedAt, DateTime::from_nanos(updated)),
		];
		stamps.extend(time.map(|time| (SystemColumn::Time, DateTime::from_nanos(time))));
		stamped(rn as i32, rn, stamps)
	}

	fn feed(op: &mut TakeOperator, txn: &mut DeferredTransaction, cols: RecordBatch) -> Vec<Diff> {
		let operator = op.plan.operator;
		let change = Change::from_flow(
			operator,
			ChangeVersion::from(CommitVersion(1)),
			vec![Diff::insert(cols)],
			DateTime::from_nanos(0),
		);
		op.apply(&mut TxnHostContext::new(txn, operator), change).unwrap().diffs.to_vec()
	}

	fn removed(diffs: &[Diff]) -> Vec<u64> {
		diffs.iter()
			.filter_map(|d| match d {
				Diff::Remove {
					pre,
					..
				} => Some(row_numbers(pre).unwrap().iter().map(|r| r.0).collect::<Vec<_>>()),
				_ => None,
			})
			.flatten()
			.collect()
	}

	fn inserted(diffs: &[Diff]) -> Vec<u64> {
		diffs.iter()
			.filter_map(|d| match d {
				Diff::Insert {
					post,
					..
				} => Some(row_numbers(post).unwrap().iter().map(|r| r.0).collect::<Vec<_>>()),
				_ => None,
			})
			.flatten()
			.collect()
	}

	#[test]
	fn a_row_older_than_the_whole_window_is_never_announced() {
		// Admitting a doomed late arrival and evicting it in the same change hands the subscriber an insert it
		// has to undo one diff later, so it must produce no diff at all.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let mut op = TakeOperator::new(None, OperatorId(1), 2);

		feed(&mut op, &mut txn, row(10, 10, 1_000));
		feed(&mut op, &mut txn, row(11, 11, 2_000));
		let diffs = feed(&mut op, &mut txn, row(7, 7, 500));

		assert!(diffs.is_empty(), "a row older than every live row must produce no diff, got {:?}", diffs);
	}

	#[test]
	fn a_newest_first_feed_never_evicts_a_row_it_just_admitted() {
		// This is the hydration order: rows arrive youngest first. Keying the window on arrival makes the
		// first row look oldest, so the third feed evicts the newest row instead of ignoring the oldest.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let mut op = TakeOperator::new(None, OperatorId(1), 2);

		let first = feed(&mut op, &mut txn, row(3, 3, 3_000));
		let second = feed(&mut op, &mut txn, row(2, 2, 2_000));
		let third = feed(&mut op, &mut txn, row(1, 1, 1_000));

		assert_eq!(inserted(&first), vec![3]);
		assert_eq!(inserted(&second), vec![2]);
		assert!(third.is_empty(), "the oldest row cannot enter a full window, got {:?}", third);
		assert!(
			removed(&first).is_empty() && removed(&second).is_empty() && removed(&third).is_empty(),
			"nothing may be evicted while the window is filling from newest to oldest"
		);
	}

	#[test]
	fn every_stamp_survives_the_take_row_round_trip_unchanged() {
		// The pod body carries no header, so every stamp must survive in the envelope or it is lost.
		let cols = stamped_row(7, 1_000, 2_000, Some(3_000));
		let shape = row_shape_from_columns(&cols).unwrap();
		let encoded = encode_take_rows(&shape, &cols).unwrap().remove(0);
		let decoded = decode_take_rows(&shape, &[(RowNumber(7), encoded)]).unwrap();

		assert_eq!(created_at(&decoded).unwrap(), &[DateTime::from_nanos(1_000)]);
		assert_eq!(updated_at(&decoded).unwrap(), &[DateTime::from_nanos(2_000)]);
		assert_eq!(time(&decoded).unwrap(), &[DateTime::from_nanos(3_000)]);
		assert_eq!(row_numbers(&decoded).unwrap(), &[RowNumber(7)]);
		assert_eq!(user_views(&decoded).unwrap()[0].get_value(0), Value::Int4(7));
	}

	#[test]
	fn a_row_without_a_time_round_trips_with_time_absent_and_both_stamps_present() {
		// A timeless source must never gain a fabricated #time, and must still hand the sink a created_at.
		let cols = stamped_row(9, 4_000, 5_000, None);
		let shape = row_shape_from_columns(&cols).unwrap();
		let encoded = encode_take_rows(&shape, &cols).unwrap().remove(0);
		let decoded = decode_take_rows(&shape, &[(RowNumber(9), encoded)]).unwrap();

		assert!(
			system_column(&decoded, SystemColumn::Time).is_none(),
			"a source row with no #time must not gain one, got {:?}",
			time(&decoded)
		);
		assert_eq!(created_at(&decoded).unwrap(), &[DateTime::from_nanos(4_000)]);
		assert_eq!(updated_at(&decoded).unwrap(), &[DateTime::from_nanos(5_000)]);
	}

	#[test]
	fn the_take_envelope_header_is_twenty_five_bytes_with_a_time_and_seventeen_without() {
		// The envelope must charge only for the fields set, otherwise a timeless row pays for a slot.
		let timed = stamped_row(1, 1_000, 2_000, Some(3_000));
		let timeless = stamped_row(2, 1_000, 2_000, None);
		let timed_bytes = encode_take_rows(&row_shape_from_columns(&timed).unwrap(), &timed).unwrap().remove(0);
		let timeless_bytes =
			encode_take_rows(&row_shape_from_columns(&timeless).unwrap(), &timeless).unwrap().remove(0);

		assert_eq!(Envelope::try_view(EncodedPodRow::view(&timed_bytes)).unwrap().header_size(), 25);
		assert_eq!(Envelope::try_view(EncodedPodRow::view(&timeless_bytes)).unwrap().header_size(), 17);
	}

	#[test]
	fn an_evicted_row_reaches_the_subscriber_with_its_stamps_intact() {
		// The remove diff is rebuilt from stored bytes, so only here does a lossy round trip surface.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let mut op = TakeOperator::new(None, OperatorId(1), 2);

		feed(&mut op, &mut txn, row(1, 1, 1_000));
		feed(&mut op, &mut txn, row(2, 2, 2_000));
		let diffs = feed(&mut op, &mut txn, row(3, 3, 3_000));

		assert_eq!(removed(&diffs), vec![1]);
		let Some(Diff::Remove {
			pre,
			..
		}) = diffs.iter().find(|d| matches!(d, Diff::Remove { .. }))
		else {
			panic!("admitting a newer row into a full window must evict the oldest one");
		};
		assert_eq!(created_at(pre).unwrap(), &[DateTime::from_nanos(1_000)]);
		assert_eq!(updated_at(pre).unwrap(), &[DateTime::from_nanos(1_000)]);
		assert_eq!(time(pre).unwrap(), &[DateTime::from_nanos(1_000)]);
	}
}
