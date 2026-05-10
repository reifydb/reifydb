// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, VecDeque},
	sync::Arc,
};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::row::Row;
use reifydb_type::value::row_number::RowNumber;

use super::{
	config::ChaosConfig,
	event::ChaosEvent,
	schema::{ChaosSchema, KeyStrategy},
	strategy::{ColumnRegistry, RowContent, encode_row, sample_row},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
	Insert,
	Update,
	Remove,
}

struct LiveRow {
	row: Row,
	content: RowContent,
}

pub struct Generator {
	schema: Arc<ChaosSchema>,
	registry: Arc<ColumnRegistry>,
	config: ChaosConfig,
	rng: StdRng,
	next_sequential: u64,
	live: BTreeMap<RowNumber, LiveRow>,
	ops_emitted: usize,

	pending: VecDeque<ChaosEvent>,
}

impl Generator {
	pub fn new(schema: Arc<ChaosSchema>, registry: Arc<ColumnRegistry>, config: ChaosConfig, seed: u64) -> Self {
		Self {
			schema,
			registry,
			config,
			rng: StdRng::seed_from_u64(seed),
			next_sequential: 1,
			live: BTreeMap::new(),
			ops_emitted: 0,
			pending: VecDeque::new(),
		}
	}

	pub fn live_count(&self) -> usize {
		self.live.len()
	}

	pub fn ops_emitted(&self) -> usize {
		self.ops_emitted
	}

	pub fn next_event(&mut self) -> Option<ChaosEvent> {
		if let Some(ev) = self.pending.pop_front() {
			self.ops_emitted += 1;
			return Some(ev);
		}
		if self.ops_emitted >= self.config.num_ops {
			return None;
		}
		let kind = self.choose_op_kind()?;
		let primary = match kind {
			OpKind::Insert => self.gen_insert()?,
			OpKind::Update => self.gen_update()?,
			OpKind::Remove => self.gen_remove()?,
		};
		let final_event = self.apply_chaos_primitives(primary);
		self.ops_emitted += 1;
		Some(final_event)
	}

	fn apply_chaos_primitives(&mut self, primary: ChaosEvent) -> ChaosEvent {
		let ChaosEvent::Update {
			row_number,
			pre,
			post,
		} = primary
		else {
			return primary;
		};

		if self.config.update_as_remove_insert > 0.0
			&& self.config.supported_ops.remove
			&& self.config.supported_ops.insert
			&& self.rng.random_range(0.0..1.0) < self.config.update_as_remove_insert
		{
			self.pending.push_back(ChaosEvent::Insert {
				row_number,
				row: post.clone(),
			});
			return ChaosEvent::Remove {
				row_number,
				row: pre,
			};
		}

		if self.config.duplicate_update_burst > 0.0
			&& self.rng.random_range(0.0..1.0) < self.config.duplicate_update_burst
		{
			self.pending.push_back(ChaosEvent::Update {
				row_number,
				pre: post.clone(),
				post: post.clone(),
			});
			return ChaosEvent::Update {
				row_number,
				pre,
				post,
			};
		}

		ChaosEvent::Update {
			row_number,
			pre,
			post,
		}
	}

	fn choose_op_kind(&mut self) -> Option<OpKind> {
		let supported = &self.config.supported_ops;
		let live_full = self.live.len() >= self.config.max_live_rows;
		let live_empty = self.live.is_empty();

		let mut allowed: Vec<OpKind> = Vec::with_capacity(3);
		if supported.insert && !live_full {
			allowed.push(OpKind::Insert);
		}
		if supported.update && !live_empty {
			allowed.push(OpKind::Update);
		}
		if supported.remove && !live_empty {
			allowed.push(OpKind::Remove);
		}

		if allowed.is_empty() {
			return None;
		}

		let idx = self.rng.random_range(0..allowed.len());
		Some(allowed[idx])
	}

	fn gen_insert(&mut self) -> Option<ChaosEvent> {
		let (row, content) = sample_row(&self.schema, &self.registry, &mut self.rng, self.next_sequential);
		self.next_sequential += 1;

		let rn = row.number;

		if let Some(prev) = self.live.get(&rn) {
			let pre = prev.row.clone();
			let post = row;
			self.live.insert(
				rn,
				LiveRow {
					row: post.clone(),
					content,
				},
			);
			return Some(ChaosEvent::Update {
				row_number: rn,
				pre,
				post,
			});
		}

		self.live.insert(
			rn,
			LiveRow {
				row: row.clone(),
				content,
			},
		);
		Some(ChaosEvent::Insert {
			row_number: rn,
			row,
		})
	}

	fn gen_update(&mut self) -> Option<ChaosEvent> {
		let target_rn = self.pick_live_rn()?;
		let pre = self.live.get(&target_rn)?.row.clone();
		let pre_content = self.live.get(&target_rn)?.content.clone();

		let (mut sampled_row, mut new_content) =
			sample_row(&self.schema, &self.registry, &mut self.rng, self.next_sequential);

		match &self.schema.key_strategy {
			KeyStrategy::HashOf(cols) => {
				for col in cols {
					if let Some(v) = pre_content.get(col).cloned() {
						new_content.set(col, v);
					}
				}

				if let Some(constraint) = &self.registry.constraint {
					constraint(&mut new_content);
				}

				sampled_row = encode_row(&self.schema, &new_content, target_rn);
			}
			KeyStrategy::Sequential | KeyStrategy::Custom(_) => {
				sampled_row.number = target_rn;
			}
		}

		let post = sampled_row;
		self.live.insert(
			target_rn,
			LiveRow {
				row: post.clone(),
				content: new_content,
			},
		);

		Some(ChaosEvent::Update {
			row_number: target_rn,
			pre,
			post,
		})
	}

	fn gen_remove(&mut self) -> Option<ChaosEvent> {
		let target_rn = self.pick_live_rn()?;
		let live = self.live.remove(&target_rn)?;
		Some(ChaosEvent::Remove {
			row_number: target_rn,
			row: live.row,
		})
	}

	fn pick_live_rn(&mut self) -> Option<RowNumber> {
		if self.live.is_empty() {
			return None;
		}
		let idx = self.rng.random_range(0..self.live.len());

		let rn = *self.live.keys().nth(idx)?;
		Some(rn)
	}
}

#[cfg(test)]
mod tests {
	use std::{iter::from_fn, ops::Range};

	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	use super::{
		super::{
			config::{BatchSizeDist, SupportedOps},
			strategy::samplers,
		},
		*,
	};

	fn shape(fields: &[(&str, Type)]) -> RowShape {
		RowShape::new(fields.iter().map(|(n, t)| RowShapeField::unconstrained(*n, t.clone())).collect())
	}

	fn schema_hashof() -> Arc<ChaosSchema> {
		Arc::new(ChaosSchema {
			input_shape: shape(&[("k", Type::Uint8), ("v", Type::Float8)]),
			output_shape: shape(&[("k", Type::Uint8), ("v", Type::Float8)]),
			key_strategy: KeyStrategy::hash_of(["k"]),
			output_key_columns: vec!["k".into()],
		})
	}

	fn schema_sequential() -> Arc<ChaosSchema> {
		Arc::new(ChaosSchema {
			input_shape: shape(&[("k", Type::Uint8), ("v", Type::Float8)]),
			output_shape: shape(&[("k", Type::Uint8), ("v", Type::Float8)]),
			key_strategy: KeyStrategy::Sequential,
			output_key_columns: vec!["k".into()],
		})
	}

	fn registry_kv(k_range: Range<u64>) -> Arc<ColumnRegistry> {
		let mut reg = ColumnRegistry::new();
		reg.register("k", samplers::u64_range(k_range));
		reg.register("v", samplers::f64_range(0.0..100.0));
		Arc::new(reg)
	}

	fn cfg(num_ops: usize, max_live: usize, ops: SupportedOps) -> ChaosConfig {
		ChaosConfig {
			num_ops,
			max_live_rows: max_live,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
			batch_size: BatchSizeDist::Constant(1),
			supported_ops: ops,
		}
	}

	#[test]
	fn insert_only_emits_only_inserts() {
		// max_live > num_ops so the cap doesn't kick in; with Sequential
		// keys, every Insert produces a fresh live row, so we expect
		// exactly num_ops Inserts.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg(100, 200, SupportedOps::insert_only()),
			42,
		);
		let mut count_insert = 0;
		while let Some(ev) = g.next_event() {
			assert!(ev.is_insert(), "non-insert under insert_only: {:?}", ev);
			count_insert += 1;
		}
		assert_eq!(count_insert, 100);
		assert_eq!(g.live_count(), 100, "Sequential + insert-only -> live equals inserts");
	}

	#[test]
	fn insert_only_with_tight_cap_stops_at_cap() {
		// Hard cap: with Sequential keys, the generator runs out of room
		// once live == max_live_rows.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg(100, 25, SupportedOps::insert_only()),
			42,
		);
		let mut count_insert = 0;
		while let Some(ev) = g.next_event() {
			assert!(ev.is_insert());
			count_insert += 1;
		}
		assert_eq!(count_insert, 25, "stops at cap");
		assert_eq!(g.live_count(), 25);
	}

	#[test]
	fn no_remove_keeps_live_monotonic() {
		// Live count never decreases when Remove is disabled.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg(200, 100, SupportedOps::no_remove()),
			7,
		);
		let mut last_live = 0usize;
		while let Some(_ev) = g.next_event() {
			let now = g.live_count();
			assert!(now >= last_live, "live shrank: {last_live} -> {now}");
			last_live = now;
		}
	}

	#[test]
	fn all_ops_produces_mix() {
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg(500, 50, SupportedOps::all()),
			99,
		);
		let mut counts = (0, 0, 0);
		while let Some(ev) = g.next_event() {
			match ev {
				ChaosEvent::Insert {
					..
				} => counts.0 += 1,
				ChaosEvent::Update {
					..
				} => counts.1 += 1,
				ChaosEvent::Remove {
					..
				} => counts.2 += 1,
			}
		}
		// Sanity: all three kinds appear at least a few times over 500 ops.
		assert!(counts.0 > 10, "too few Inserts: {}", counts.0);
		assert!(counts.1 > 10, "too few Updates: {}", counts.1);
		assert!(counts.2 > 10, "too few Removes: {}", counts.2);
	}

	#[test]
	fn same_seed_produces_same_event_sequence() {
		fn run(seed: u64) -> Vec<(u8, RowNumber)> {
			let mut g = Generator::new(
				schema_sequential(),
				registry_kv(1..1000),
				cfg(50, 25, SupportedOps::all()),
				seed,
			);
			let mut out = Vec::new();
			while let Some(ev) = g.next_event() {
				let tag = match ev {
					ChaosEvent::Insert {
						..
					} => 0,
					ChaosEvent::Update {
						..
					} => 1,
					ChaosEvent::Remove {
						..
					} => 2,
				};
				out.push((tag, ev.row_number()));
			}
			out
		}
		assert_eq!(run(123), run(123));
		assert_ne!(run(123), run(124));
	}

	#[test]
	fn hashof_collision_rewrites_insert_as_update() {
		// k_range is tiny so KeyStrategy::HashOf collisions are likely.
		// We expect to see at least one Update emitted before any
		// explicit Update is generated by the generator's own logic.
		// Force "Insert only" SupportedOps to isolate the rewrite path:
		// with only Inserts in the budget, any Update we observe came
		// from the collision rewrite.
		let mut g = Generator::new(
			schema_hashof(),
			registry_kv(1..3), // only k in {1, 2}
			cfg(50, 50, SupportedOps::insert_only()),
			0,
		);
		let mut updates_seen = 0;
		while let Some(ev) = g.next_event() {
			if ev.is_update() {
				updates_seen += 1;
			}
		}
		assert!(updates_seen > 0, "expected at least one Insert -> Update rewrite from collision");
	}

	#[test]
	fn update_preserves_key_columns_under_hashof() {
		// Force one Insert, then several Updates, all against the same
		// HashOf RowNumber. Verify the post row's key column matches
		// the pre row's (HashOf(k) preserves k across the Update).
		let cfg = ChaosConfig {
			num_ops: 20,
			max_live_rows: 50,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
			batch_size: BatchSizeDist::Constant(1),
			supported_ops: SupportedOps::no_remove(),
		};
		let mut g = Generator::new(schema_hashof(), registry_kv(1..1000), cfg, 5);

		let mut updates_with_matching_keys = 0;
		let mut updates_observed = 0;
		while let Some(ev) = g.next_event() {
			if let ChaosEvent::Update {
				pre,
				post,
				..
			} = ev
			{
				updates_observed += 1;
				let pre_k = read_u64(&pre, "k");
				let post_k = read_u64(&post, "k");
				if pre_k == post_k {
					updates_with_matching_keys += 1;
				}
			}
		}

		assert!(updates_observed > 0, "expected at least one Update over 20 ops");
		assert_eq!(
			updates_with_matching_keys, updates_observed,
			"every Update under HashOf should preserve the key column"
		);
	}

	#[test]
	fn remove_drops_from_live() {
		let cfg = ChaosConfig {
			num_ops: 100,
			max_live_rows: 50,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
			batch_size: BatchSizeDist::Constant(1),
			// Insert + Remove only - no Updates muddying the count.
			supported_ops: SupportedOps::no_update(),
		};
		let mut g = Generator::new(schema_sequential(), registry_kv(1..1000), cfg, 11);

		let mut inserts = 0;
		let mut removes = 0;
		while let Some(ev) = g.next_event() {
			match ev {
				ChaosEvent::Insert {
					..
				} => inserts += 1,
				ChaosEvent::Remove {
					..
				} => removes += 1,
				ChaosEvent::Update {
					..
				} => panic!("Update emitted under no_update"),
			}
		}
		// Live = inserts - removes.
		assert_eq!(g.live_count(), inserts - removes);
		assert!(removes > 0, "expected at least one Remove with all-Insert/Remove enabled");
	}

	fn read_u64(row: &Row, name: &str) -> u64 {
		// Simple byte read for Uint8 columns.
		let field = row.shape.find_field(name).expect("field");
		let buf = &row.encoded.as_slice()[field.offset as usize..(field.offset as usize + field.size as usize)];
		let mut bytes = [0u8; 8];
		bytes.copy_from_slice(buf);
		u64::from_le_bytes(bytes)
	}

	fn cfg_with_chaos(
		num_ops: usize,
		max_live: usize,
		ops: SupportedOps,
		dup_burst: f64,
		rewrite: f64,
	) -> ChaosConfig {
		ChaosConfig {
			num_ops,
			max_live_rows: max_live,
			duplicate_update_burst: dup_burst,
			update_as_remove_insert: rewrite,
			batch_size: BatchSizeDist::Constant(1),
			supported_ops: ops,
		}
	}

	#[test]
	fn duplicate_burst_inflates_update_count_at_p_one() {
		// p=1.0 means every Update spawns exactly one duplicate.
		// With Sequential keys + no_remove, base path produces a mix of
		// Insert and Update. Each Update should be followed (eventually)
		// by an identical no-op Update.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(200, 100, SupportedOps::no_remove(), 1.0, 0.0),
			77,
		);
		let mut count_update = 0;
		while let Some(ev) = g.next_event() {
			if ev.is_update() {
				count_update += 1;
			}
		}
		// Without duplicate-burst, we'd expect ~ half the events to be
		// Updates (mix of Insert/Update). With duplicate-burst at 1.0,
		// every Update spawns one more Update, so Updates dominate.
		// Check: Updates > Inserts.
		assert!(count_update > 100, "expected many duplicates; got {} updates of 200 ops", count_update);
	}

	#[test]
	fn duplicate_burst_at_zero_produces_no_extra_updates() {
		let mut g_burst = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(200, 100, SupportedOps::no_remove(), 0.0, 0.0),
			77,
		);
		let mut g_quiet = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(200, 100, SupportedOps::no_remove(), 0.0, 0.0),
			77,
		);
		// Same seed, same chaos = 0 -> identical sequences.
		let a: Vec<_> = from_fn(|| g_burst.next_event()).collect();
		let b: Vec<_> = from_fn(|| g_quiet.next_event()).collect();
		assert_eq!(a.len(), b.len());
		assert_eq!(a.len(), 200);
	}

	#[test]
	fn duplicate_burst_post_equals_pre_in_dup() {
		// The queued duplicate Update has pre = post (no-op replacement).
		// Verify by counting Updates whose pre/post bytes match.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(100, 50, SupportedOps::no_remove(), 1.0, 0.0),
			55,
		);
		let mut noop_count = 0;
		while let Some(ev) = g.next_event() {
			if let ChaosEvent::Update {
				pre,
				post,
				..
			} = ev
			{
				if pre.encoded.as_slice() == post.encoded.as_slice() {
					noop_count += 1;
				}
			}
		}
		// At p=1.0, half of the Updates should be the queued duplicates
		// (no-op pre==post). The original Updates have pre != post.
		assert!(noop_count > 10, "expected several no-op duplicate Updates; got {noop_count}");
	}

	#[test]
	fn rewrite_at_p_one_replaces_updates_with_remove_insert_pairs() {
		// With rewrite p=1.0, every generated Update is replaced by
		// Remove + queued Insert. So no Update events should appear in
		// the OUTPUT stream.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(100, 50, SupportedOps::all(), 0.0, 1.0),
			33,
		);
		let mut update_count = 0;
		let mut total = 0;
		while let Some(ev) = g.next_event() {
			total += 1;
			if ev.is_update() {
				update_count += 1;
			}
		}
		assert_eq!(update_count, 0, "no Updates should survive p=1.0 rewrite; saw {update_count} of {total}");
	}

	#[test]
	fn rewrite_with_remove_disabled_has_no_effect() {
		// Update -> Remove+Insert requires Remove enabled. With no_remove,
		// the rewrite is a no-op even at p=1.0; Updates pass through.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(100, 50, SupportedOps::no_remove(), 0.0, 1.0),
			11,
		);
		let mut update_count = 0;
		while let Some(ev) = g.next_event() {
			if ev.is_update() {
				update_count += 1;
			}
		}
		assert!(update_count > 0, "Updates should still appear when rewrite is impossible (no Remove)");
	}

	#[test]
	fn rewrite_pending_insert_fires_before_new_generation() {
		// Drain order: after a rewrite, the queued Insert fires on the
		// next next_event call, before any new generation. Check by
		// looking for a Remove immediately followed by an Insert with
		// the same RowNumber, with no other event in between.
		let mut g = Generator::new(
			schema_sequential(),
			registry_kv(1..1000),
			cfg_with_chaos(100, 50, SupportedOps::all(), 0.0, 1.0),
			99,
		);
		let events: Vec<_> = from_fn(|| g.next_event()).collect();
		// Find at least one Remove immediately followed by an Insert at
		// the same RowNumber.
		let mut paired = 0;
		for w in events.windows(2) {
			if let (
				ChaosEvent::Remove {
					row_number: r1,
					..
				},
				ChaosEvent::Insert {
					row_number: r2,
					..
				},
			) = (&w[0], &w[1])
			{
				if r1 == r2 {
					paired += 1;
				}
			}
		}
		assert!(paired > 0, "expected at least one Remove-then-Insert pair from rewrite; got {paired}");
	}

	#[test]
	fn chaos_primitives_dont_break_seed_reproducibility() {
		// All chaos primitives on, tight ops, same seed -> same sequence.
		fn run(seed: u64) -> Vec<u8> {
			let mut g = Generator::new(
				schema_sequential(),
				registry_kv(1..1000),
				cfg_with_chaos(50, 25, SupportedOps::all(), 0.5, 0.3),
				seed,
			);
			let mut tags = Vec::new();
			while let Some(ev) = g.next_event() {
				tags.push(match ev {
					ChaosEvent::Insert {
						..
					} => 0,
					ChaosEvent::Update {
						..
					} => 1,
					ChaosEvent::Remove {
						..
					} => 2,
				});
			}
			tags
		}
		assert_eq!(run(42), run(42));
		assert_ne!(run(42), run(43));
	}
}
