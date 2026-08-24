// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use rand::rngs::StdRng;
use reifydb_core::{interface::change::Change, row::Row};
use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	event::{ChaosBatch, ChaosEvent},
	expectation::ViewClaim,
	model::Model,
	view::{MaterializedView, OutputKey},
	workload::{Lanes, Workload},
};
use reifydb_value::value::row_number::RowNumber;

use super::{
	context::ChaosContext,
	schema::ChaosSchema,
	strategy::{ColumnRegistry, RowContent, encode_row, sample_row},
};
use crate::builders::TestChangeBuilder;

#[derive(Clone, Debug)]
pub struct GuestRow {
	pub row: Row,
	pub content: RowContent,
}

pub struct SamplerWorkload {
	schema: Arc<ChaosSchema>,
	registry: Arc<ColumnRegistry>,
	projection: Vec<usize>,
}

impl SamplerWorkload {
	pub fn new(schema: Arc<ChaosSchema>, registry: Arc<ColumnRegistry>) -> Self {
		let projection = (0..schema.output_shape.fields().len()).collect();
		Self {
			schema,
			registry,
			projection,
		}
	}

	fn pinned_columns(&self) -> Vec<String> {
		let mut columns: Vec<String> = self.schema.key_strategy.deriving_columns().to_vec();
		for column in &self.schema.output_key_columns {
			if !columns.contains(column) {
				columns.push(column.clone());
			}
		}
		columns
	}
}

impl Workload for SamplerWorkload {
	type Row = GuestRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> GuestRow {
		let (row, content) = sample_row(&self.schema, &self.registry, rng, number.0);
		GuestRow {
			row,
			content,
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &GuestRow) -> GuestRow {
		let target = row.row.number;
		let (_, mut content) = sample_row(&self.schema, &self.registry, rng, target.0);

		for column in self.pinned_columns() {
			if let Some(value) = row.content.get(&column).cloned() {
				content.set(column, value);
			}
		}
		if let Some(constraint) = &self.registry.constraint {
			constraint(&mut content);
		}
		GuestRow {
			row: encode_row(&self.schema, &content, target),
			content,
		}
	}

	fn adopt(&self, live: &GuestRow, incoming: &GuestRow) -> GuestRow {
		GuestRow {
			row: encode_row(&self.schema, &incoming.content, live.row.number),
			content: incoming.content.clone(),
		}
	}

	fn lanes(&self, row: &GuestRow) -> Lanes {
		Lanes {
			number: row.row.number.0,
			group: 0,
			coord: 0,
			value: 0,
		}
	}

	fn insert(&self, rows: &[GuestRow]) -> Change {
		let mut builder = TestChangeBuilder::new();
		for r in rows {
			builder = builder.insert(r.row.clone());
		}
		builder.build()
	}

	fn remove(&self, row: &GuestRow) -> Change {
		TestChangeBuilder::new().remove(row.row.clone()).build()
	}

	fn update(&self, pre: &GuestRow, post: &GuestRow) -> Change {
		TestChangeBuilder::new().update(pre.row.clone(), post.row.clone()).build()
	}

	fn projection(&self) -> &[usize] {
		&self.projection
	}

	fn identity(&self, row: &GuestRow) -> Option<OutputKey> {
		let mut values = Vec::with_capacity(self.schema.output_key_columns.len());
		for column in &self.schema.output_key_columns {
			values.push(row.content.get(column).cloned()?);
		}
		Some(OutputKey::new(values))
	}
}

pub type OracleFn = Arc<dyn Fn(&ChaosContext, &[ChaosBatch]) -> MaterializedView + Send + Sync>;

pub struct OracleClaim {
	pub context: ChaosContext,
	pub oracle: OracleFn,
	pub key_columns: Vec<String>,
	pub tolerances: Tolerances,
}

pub struct ReplayModel {
	batches: Vec<ChaosBatch>,
	pending: Vec<ChaosEvent>,
	claim: Option<OracleClaim>,
	drain_floor_ms: u64,
}

impl Default for ReplayModel {
	fn default() -> Self {
		Self::new()
	}
}

impl ReplayModel {
	pub fn new() -> Self {
		Self {
			batches: Vec::new(),
			pending: Vec::new(),
			claim: None,
			drain_floor_ms: 0,
		}
	}

	pub fn claiming(claim: OracleClaim) -> Self {
		Self {
			claim: Some(claim),
			..Self::new()
		}
	}

	pub fn into_log(mut self) -> Vec<ChaosBatch> {
		self.close_batch();
		self.batches
	}

	pub fn log(&self) -> Vec<ChaosBatch> {
		let mut out = self.batches.clone();
		if !self.pending.is_empty() {
			out.push(ChaosBatch::new(self.pending.clone()));
		}
		out
	}

	fn close_batch(&mut self) {
		if !self.pending.is_empty() {
			self.batches.push(ChaosBatch::new(mem::take(&mut self.pending)));
		}
	}

	fn observe(&mut self, row: &Row) {
		if let Some(at) = row.shape.time(&row.encoded).map(|time| time.to_epoch_millis()).filter(|&ms| ms > 0) {
			self.drain_floor_ms = self.drain_floor_ms.max(at as u64);
		}
	}
}

impl Model<GuestRow> for ReplayModel {
	type Expectation = Option<ViewClaim>;

	fn admit(&mut self, row: &GuestRow) -> bool {
		self.observe(&row.row);
		self.pending.push(ChaosEvent::Insert {
			row_number: row.row.number,
			row: row.row.clone(),
		});
		true
	}

	fn retract(&mut self, row: &GuestRow) {
		self.observe(&row.row);
		self.pending.push(ChaosEvent::Remove {
			row_number: row.row.number,
			row: row.row.clone(),
		});
	}

	fn update(&mut self, pre: &GuestRow, post: &GuestRow) {
		self.observe(&post.row);
		self.pending.push(ChaosEvent::Update {
			row_number: post.row.number,
			pre: pre.row.clone(),
			post: post.row.clone(),
		});
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn live(&self) -> Option<ViewClaim> {
		None
	}

	fn all(&self) -> Option<ViewClaim> {
		None
	}

	fn after_drain(&self) -> Option<ViewClaim> {
		let claim = self.claim.as_ref()?;
		let log = self.log();
		Some(ViewClaim::new(
			(claim.oracle)(&claim.context, &log),
			claim.key_columns.clone(),
			claim.tolerances.clone(),
		))
	}

	fn step_complete(&mut self) {
		self.close_batch();
	}

	fn drain_floor(&self) -> u64 {
		self.drain_floor_ms
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Range;

	use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
	use reifydb_core::interface::change::DiffType;
	use reifydb_testing_chaos::operator::{
		drive::drive,
		scenario::{BatchSize, Scenario, SupportedOps},
		subject::Subject,
	};
	use reifydb_value::{
		Result,
		value::{Value, value_type::ValueType},
	};

	use super::*;
	use crate::chaos::{schema::KeyStrategy, strategy::samplers};

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	enum Kind {
		Insert,
		Update,
		Remove,
	}

	#[derive(Debug, Clone)]
	struct Recorded {
		kind: Kind,
		pre: Option<GuestRow>,
		post: Option<GuestRow>,
	}

	impl Recorded {
		fn number(&self) -> u64 {
			self.post.as_ref().or(self.pre.as_ref()).expect("an event carries a row").row.number.0
		}
	}

	/// Swallows every change and publishes nothing, so the run exercises generation alone.
	struct NullSubject {
		applied: Vec<Change>,
	}

	impl Subject for NullSubject {
		fn apply(&mut self, change: Change) -> Result<Change> {
			self.applied.push(change);
			Ok(TestChangeBuilder::new().build())
		}

		fn tick(&mut self, _at_ms: u64) -> Result<Option<Change>> {
			Ok(None)
		}
	}

	/// Claims nothing about the view, so every bound holds against a subject that publishes nothing.
	struct RecordingModel {
		events: Vec<Recorded>,
	}

	impl Model<GuestRow> for RecordingModel {
		type Expectation = Vec<Vec<Value>>;

		fn admit(&mut self, row: &GuestRow) -> bool {
			self.events.push(Recorded {
				kind: Kind::Insert,
				pre: None,
				post: Some(row.clone()),
			});
			true
		}

		fn retract(&mut self, row: &GuestRow) {
			self.events.push(Recorded {
				kind: Kind::Remove,
				pre: Some(row.clone()),
				post: None,
			});
		}

		fn update(&mut self, pre: &GuestRow, post: &GuestRow) {
			self.events.push(Recorded {
				kind: Kind::Update,
				pre: Some(pre.clone()),
				post: Some(post.clone()),
			});
		}

		fn advance_ledger(&mut self, _at_ms: u64) {}

		fn live(&self) -> Self::Expectation {
			Vec::new()
		}

		fn all(&self) -> Self::Expectation {
			Vec::new()
		}

		fn after_drain(&self) -> Self::Expectation {
			Vec::new()
		}
	}

	fn shape(fields: &[(&str, ValueType)]) -> RowShape {
		RowShape::new(
			RowFamily::Table,
			fields.iter().map(|(n, t)| RowShapeField::unconstrained(*n, t.clone())).collect(),
		)
	}

	fn schema_with(key_strategy: KeyStrategy) -> Arc<ChaosSchema> {
		Arc::new(ChaosSchema {
			input_shape: shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8)]),
			output_shape: shape(&[("k", ValueType::Uint8), ("v", ValueType::Float8)]),
			key_strategy,
			output_key_columns: vec!["k".into()],
			time_column: None,
		})
	}

	fn schema_hashof() -> Arc<ChaosSchema> {
		schema_with(KeyStrategy::hash_of(["k"]))
	}

	fn schema_sequential() -> Arc<ChaosSchema> {
		schema_with(KeyStrategy::Sequential)
	}

	fn registry_kv(k_range: Range<u64>) -> Arc<ColumnRegistry> {
		let mut reg = ColumnRegistry::new();
		reg.register("k", samplers::u64_range(k_range));
		reg.register("v", samplers::f64_range(0.0..100.0));
		Arc::new(reg)
	}

	fn cfg(steps: u32, max_live: usize, ops: SupportedOps) -> Scenario {
		cfg_with_chaos(steps, max_live, ops, 0.0, 0.0)
	}

	fn cfg_with_chaos(steps: u32, max_live: usize, ops: SupportedOps, dup_burst: f64, rewrite: f64) -> Scenario {
		Scenario::mixed(steps)
			.with_ops(ops)
			.with_max_live(max_live)
			.with_batch(BatchSize::Constant(1))
			.with_duplicate_update_burst(dup_burst)
			.with_update_as_remove_insert(rewrite)
	}

	fn run(
		schema: Arc<ChaosSchema>,
		registry: Arc<ColumnRegistry>,
		scenario: Scenario,
		seed: u64,
	) -> Vec<Recorded> {
		run_with_changes(schema, registry, scenario, seed).0
	}

	fn run_with_changes(
		schema: Arc<ChaosSchema>,
		registry: Arc<ColumnRegistry>,
		scenario: Scenario,
		seed: u64,
	) -> (Vec<Recorded>, Vec<Change>) {
		let workload = SamplerWorkload::new(schema, registry);
		let mut subject = NullSubject {
			applied: Vec::new(),
		};
		let mut model = RecordingModel {
			events: Vec::new(),
		};
		let driven = drive(seed, scenario, &mut subject, &workload, &mut model);
		assert!(
			driven.divergence.is_none(),
			"a null subject publishes nothing, so no bound can be violated - a failure here is a \
			 driver defect: {:?}",
			driven.divergence
		);
		(model.events, subject.applied)
	}

	fn counts(events: &[Recorded]) -> (usize, usize, usize) {
		let tally = |kind| events.iter().filter(|e| e.kind == kind).count();
		(tally(Kind::Insert), tally(Kind::Update), tally(Kind::Remove))
	}

	fn replay_live(events: &[Recorded]) -> Vec<u64> {
		// Replays the stream the way a keyed consumer would, so an incoherent log is caught here.
		let mut live: Vec<u64> = Vec::new();
		for event in events {
			let number = event.number();
			match event.kind {
				Kind::Insert => {
					assert!(
						!live.contains(&number),
						"insert of row {number} that was already live"
					);
					live.push(number);
				}
				Kind::Update => {
					assert!(live.contains(&number), "update of row {number} that was not live");
				}
				Kind::Remove => {
					let at = live.iter().position(|n| *n == number);
					let at = at
						.unwrap_or_else(|| panic!("remove of row {number} that was not live"));
					live.remove(at);
				}
			}
		}
		live
	}

	fn read_u64(row: &Row, name: &str) -> u64 {
		let field = row.shape.find_field(name).expect("field");
		let buf = &row.encoded.as_slice()[field.offset as usize..(field.offset as usize + field.size as usize)];
		let mut bytes = [0u8; 8];
		bytes.copy_from_slice(buf);
		u64::from_le_bytes(bytes)
	}

	#[test]
	fn insert_only_emits_only_inserts() {
		// max_live exceeds the step count so the ceiling never binds; with sequential keys every step
		// mints a fresh row, so the run must be all inserts and nothing else.
		let events = run(
			schema_sequential(),
			registry_kv(1..1_000_000_000_000),
			cfg(100, 200, SupportedOps::insert_only()),
			42,
		);
		assert!(events.iter().all(|e| e.kind == Kind::Insert), "a non-insert appeared under insert_only");
		assert_eq!(events.len(), 100);
		assert_eq!(
			replay_live(&events).len(),
			100,
			"sequential keys plus insert-only means live equals inserts"
		);
	}

	#[test]
	fn insert_only_with_tight_cap_stops_at_cap() {
		// max_live is a ceiling, not an eviction policy: if it silently evicted, a configuration
		// asking for 25 live rows would exercise 100.
		let events = run(
			schema_sequential(),
			registry_kv(1..1_000_000_000_000),
			cfg(100, 25, SupportedOps::insert_only()),
			42,
		);
		assert!(events.iter().all(|e| e.kind == Kind::Insert));
		assert_eq!(events.len(), 25, "inserts must stop at the ceiling");
		assert_eq!(replay_live(&events).len(), 25);
	}

	#[test]
	fn no_remove_keeps_live_monotonic() {
		// Nothing may shrink the live set when removes are disabled, including the update path - an update
		// rewritten into remove-plus-insert would violate the configuration it was run under.
		let events =
			run(schema_sequential(), registry_kv(1..1000), cfg(200, 100, SupportedOps::no_remove()), 7);
		assert!(events.iter().all(|e| e.kind != Kind::Remove), "a remove appeared under no_remove");
		let mut live = 0usize;
		for event in &events {
			if event.kind == Kind::Insert {
				live += 1;
			}
			assert!(live >= 1 || event.kind == Kind::Insert, "an update preceded every insert");
		}
		assert_eq!(replay_live(&events).len(), live);
	}

	#[test]
	fn all_ops_produces_mix() {
		let events = run(schema_sequential(), registry_kv(1..1000), cfg(500, 50, SupportedOps::all()), 99);
		let (inserts, updates, removes) = counts(&events);
		assert!(inserts > 10, "too few inserts: {inserts}");
		assert!(updates > 10, "too few updates: {updates}");
		assert!(removes > 10, "too few removes: {removes}");
		replay_live(&events);
	}

	#[test]
	fn no_update_never_emits_an_update_and_removes_drop_from_live() {
		// Insert plus remove only, so the live set is exactly inserts minus removes and any update would be
		// the driver inventing an operation the configuration forbids.
		let events = run(
			schema_sequential(),
			registry_kv(1..1_000_000_000_000),
			cfg(100, 50, SupportedOps::no_update()),
			11,
		);
		let (inserts, updates, removes) = counts(&events);
		assert_eq!(updates, 0, "an update appeared under no_update");
		assert!(removes > 0, "expected at least one remove");
		assert_eq!(replay_live(&events).len(), inserts - removes);
	}

	#[test]
	fn same_seed_produces_same_event_sequence() {
		let sequence = |seed| {
			run(schema_sequential(), registry_kv(1..1000), cfg(50, 25, SupportedOps::all()), seed)
				.iter()
				.map(|e| (e.kind, e.number()))
				.collect::<Vec<_>>()
		};
		assert_eq!(sequence(123), sequence(123));
		assert_ne!(sequence(123), sequence(124));
	}

	#[test]
	fn hashof_collision_rewrites_insert_as_update() {
		// Only two distinct keys exist, so derived row numbers collide almost immediately. With inserts as
		// the only permitted operation, every update observed can only have come from the collision path.
		let events = run(schema_hashof(), registry_kv(1..3), cfg(50, 50, SupportedOps::insert_only()), 0);
		let (_, updates, _) = counts(&events);
		assert!(updates > 0, "expected an insert over a live row to become an update");
		replay_live(&events);
	}

	#[test]
	fn update_preserves_key_columns_under_hashof() {
		// Identity is derived from the key column, so resampling it would move the row and make the
		// update indistinguishable from a delete-plus-insert of an unrelated row.
		let events = run(schema_hashof(), registry_kv(1..1000), cfg(20, 50, SupportedOps::no_remove()), 5);
		let updates: Vec<&Recorded> = events.iter().filter(|e| e.kind == Kind::Update).collect();
		assert!(!updates.is_empty(), "expected at least one update over 20 steps");
		for update in updates {
			let pre = update.pre.as_ref().expect("an update carries a pre row");
			let post = update.post.as_ref().expect("an update carries a post row");
			assert_eq!(
				read_u64(&pre.row, "k"),
				read_u64(&post.row, "k"),
				"an update under a derived key must preserve the key column"
			);
		}
	}

	#[test]
	fn duplicate_burst_at_one_pairs_every_update_with_a_no_op_repeat() {
		// The point of the burst is to publish a change that carries no information, which a consumer must
		// tolerate. At p=1 each real update is followed by exactly one repeat whose pre equals its post.
		let events = run(
			schema_sequential(),
			registry_kv(1..1_000_000_000_000),
			cfg_with_chaos(200, 100, SupportedOps::no_remove(), 1.0, 0.0),
			77,
		);
		let (noop, changed) =
			events.iter().filter(|e| e.kind == Kind::Update).fold((0, 0), |(noop, changed), e| {
				let pre = e.pre.as_ref().expect("an update carries a pre row");
				let post = e.post.as_ref().expect("an update carries a post row");
				if pre.row.encoded.as_slice() == post.row.encoded.as_slice() {
					(noop + 1, changed)
				} else {
					(noop, changed + 1)
				}
			});
		assert!(changed > 0, "expected some real updates to duplicate");
		assert_eq!(noop, changed, "at p=1 every update must spawn exactly one no-op repeat");
	}

	#[test]
	fn duplicate_burst_at_one_inflates_the_update_count() {
		let with_burst = run(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(200, 100, SupportedOps::no_remove(), 1.0, 0.0),
			77,
		);
		let without = run(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(200, 100, SupportedOps::no_remove(), 0.0, 0.0),
			77,
		);
		assert!(
			counts(&with_burst).1 > counts(&without).1,
			"bursting must publish more updates than not bursting, got {} against {}",
			counts(&with_burst).1,
			counts(&without).1
		);
	}

	#[test]
	fn duplicate_burst_at_zero_draws_nothing_and_repeats_nothing() {
		// At zero the primitive must not touch the random stream at all, otherwise turning it off would
		// still reshuffle every corpus that followed it.
		let quiet = |seed| {
			run(
				schema_sequential(),
				registry_kv(1..1000),
				cfg_with_chaos(200, 100, SupportedOps::no_remove(), 0.0, 0.0),
				seed,
			)
		};
		let a = quiet(77);
		let b = quiet(77);
		assert_eq!(
			a.iter().map(|e| (e.kind, e.number())).collect::<Vec<_>>(),
			b.iter().map(|e| (e.kind, e.number())).collect::<Vec<_>>()
		);
		let noop =
			a.iter().filter(|e| e.kind == Kind::Update)
				.filter(|e| {
					let pre = e.pre.as_ref().expect("pre");
					let post = e.post.as_ref().expect("post");
					pre.row.encoded.as_slice() == post.row.encoded.as_slice()
				})
				.count();
		assert_eq!(noop, 0, "no repeats may appear when the burst is off");
	}

	#[test]
	fn rewrite_at_one_replaces_every_update_with_a_remove_insert_pair() {
		// The same transition expressed as two diffs instead of one. An operator that handles an update but
		// mishandles the split is exactly what this primitive is looking for, so no update may survive.
		let events = run(
			schema_sequential(),
			registry_kv(1..1_000_000_000_000),
			cfg_with_chaos(100, 50, SupportedOps::all(), 0.0, 1.0),
			33,
		);
		let (_, updates, removes) = counts(&events);
		assert_eq!(updates, 0, "no update may survive a rewrite at p=1");
		assert!(removes > 0, "the rewrite must publish removes");
		replay_live(&events);
	}

	#[test]
	fn rewrite_emits_the_insert_immediately_after_its_remove() {
		// The pair must be adjacent and name the same row; a gap between them would let an unrelated
		// operation observe the row as absent, which the rewrite is not meant to simulate.
		let events = run(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(100, 50, SupportedOps::all(), 0.0, 1.0),
			99,
		);
		let paired = events
			.windows(2)
			.filter(|w| {
				w[0].kind == Kind::Remove && w[1].kind == Kind::Insert && w[0].number() == w[1].number()
			})
			.count();
		assert!(paired > 0, "expected a remove followed at once by an insert of the same row");
	}

	#[test]
	fn rewrite_with_remove_disabled_has_no_effect() {
		// Splitting an update into remove-plus-insert would publish a remove, so the primitive must stand
		// down rather than manufacture an operation the configuration disabled.
		let events = run(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(100, 50, SupportedOps::no_remove(), 0.0, 1.0),
			11,
		);
		let (_, updates, removes) = counts(&events);
		assert_eq!(removes, 0, "a rewrite must not smuggle in a remove under no_remove");
		assert!(updates > 0, "updates must pass through when the rewrite cannot apply");
	}

	#[test]
	fn chaos_primitives_dont_break_seed_reproducibility() {
		let sequence = |seed| {
			run(
				schema_sequential(),
				registry_kv(1..1000),
				cfg_with_chaos(50, 25, SupportedOps::all(), 0.5, 0.3),
				seed,
			)
			.iter()
			.map(|e| (e.kind, e.number()))
			.collect::<Vec<_>>()
		};
		assert_eq!(sequence(42), sequence(42));
		assert_ne!(sequence(42), sequence(43));
	}

	#[test]
	fn a_constant_batch_of_one_applies_a_single_row_per_change() {
		let (_, changes) = run_with_changes(
			schema_sequential(),
			registry_kv(1..1000),
			cfg(50, 200, SupportedOps::insert_only()),
			0,
		);
		assert_eq!(changes.len(), 50);
		for change in &changes {
			assert_eq!(change.diffs.len(), 1, "a constant batch of one must apply one diff per change");
		}
	}

	#[test]
	fn a_constant_batch_of_n_packs_n_rows_into_one_change() {
		// Batching is what makes a single change span a boundary, so the rows must arrive together rather
		// than as n separate changes.
		let scenario = cfg(4, 500, SupportedOps::insert_only()).with_batch(BatchSize::Constant(50));
		let (events, changes) = run_with_changes(schema_sequential(), registry_kv(1..1000), scenario, 0);
		assert_eq!(changes.len(), 4, "one change per step");
		for change in &changes {
			assert_eq!(change.diffs.len(), 50, "every row of the batch must ride one change");
		}
		assert_eq!(events.len(), 200);
	}

	#[test]
	fn the_recorded_log_matches_what_the_subject_was_asked_to_apply() {
		// Every oracle reads the model's log, so a log that drifted from the applied stream would
		// make all downstream comparisons meaningless while still looking self-consistent.
		let (events, changes) = run_with_changes(
			schema_sequential(),
			registry_kv(1..1000),
			cfg(200, 100, SupportedOps::all()),
			5,
		);
		let applied: Vec<(Kind, u64)> = changes
			.iter()
			.flat_map(|change| {
				change.diffs.iter().map(|diff| {
					let kind = match diff.kind() {
						DiffType::Insert => Kind::Insert,
						DiffType::Update => Kind::Update,
						DiffType::Remove => Kind::Remove,
					};
					let columns = diff.post().or(diff.pre()).expect("a diff carries a row");
					(kind, columns.row_numbers()[0].0)
				})
			})
			.collect();
		let recorded: Vec<(Kind, u64)> = events.iter().map(|e| (e.kind, e.number())).collect();
		assert_eq!(recorded, applied);
	}
}
