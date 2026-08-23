// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Seeded operation generator plus the per-read differential checks. State, seal anchors, checkpoints, census and
//! the operator-scoped drops all sit on one flat keyspace, so interleaving them in a single stream is what
//! exercises the paths where one subsystem's write is meant to be invisible to another's read.

use std::ops::Bound;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::GroupId,
};
use reifydb_store_operator::types::OperatorWrite;
use reifydb_testing_chaos::fuzz::{pick, run_reported, split};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::{
	fixtures::{Config, Harness, KEYSPACES, key, row},
	oracle::{AnchorRow, CensusRow, CheckpointModel, Oracle},
};

#[derive(Clone, Debug)]
pub struct Params {
	pub operators: u64,
	pub groups: u64,
	pub keyspaces: u64,
	pub suffixes: u64,
	pub flows: u64,
	pub anchor_rows: u64,
	pub sides: u8,
	pub expiry_span: u64,
	pub min_steps: u32,
	pub max_steps: u32,
	pub write_pct: u32,
	pub anchor_pct: u32,
	pub checkpoint_pct: u32,
	pub flush_pct: u32,
	pub drop_pct: u32,
	pub max_writes: u64,
	pub max_batch: u64,
	pub max_limit: u64,
}

struct State {
	oracle: Oracle,
	models: Vec<CheckpointModel>,
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let harness = Harness::new();
	let mut state = State {
		oracle: Oracle::default(),
		models: harness.configs.iter().map(|c| CheckpointModel::new(c.write_through)).collect(),
	};

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		let mut cut = p.write_pct;
		if roll < cut {
			write_step(&mut rng, &harness, &mut state, &p, step);
			continue;
		}
		cut += p.anchor_pct;
		if roll < cut {
			anchor_step(&mut rng, &harness, &mut state, &p);
			continue;
		}
		cut += p.checkpoint_pct;
		if roll < cut {
			checkpoint_step(&mut rng, &harness, &mut state, &p);
			continue;
		}
		cut += p.flush_pct;
		if roll < cut {
			harness.flush_all();
			for model in &mut state.models {
				model.flush();
			}
			continue;
		}
		cut += p.drop_pct;
		if roll < cut {
			drop_step(&mut rng, &harness, &mut state, &p);
			continue;
		}
		read_step(&mut rng, &harness, &state, &p, step);
	}

	sweep(&harness, &mut state, &p, steps);
}

fn write_step(rng: &mut StdRng, harness: &Harness, state: &mut State, p: &Params, step: u32) {
	let operator = rng.random_range(1..=p.operators);
	match rng.random_range(0u32..10) {
		0..=3 => {
			let key_bytes = random_key(rng, p);
			let value = row(operator, step as u64, step);
			state.oracle.set(operator, key_bytes.as_slice(), value.clone());
			for config in &harness.configs {
				config.store.set(OperatorId(operator), key_bytes.clone(), value.clone());
			}
		}
		4..=5 => {
			let key_bytes = random_key(rng, p);
			state.oracle.remove(operator, key_bytes.as_slice());
			for config in &harness.configs {
				config.store.remove(OperatorId(operator), &key_bytes);
			}
		}
		_ => {
			let (writes, checkpoints, deletes) = random_batch(rng, state, p, step);
			for config in &harness.configs {
				config.store.apply_batch_with_checkpoints(&writes, &checkpoints, &deletes);
			}
			for model in &mut state.models {
				for (flow, version) in &checkpoints {
					model.set(flow.0, version.0);
				}
				for flow in &deletes {
					model.delete(flow.0);
				}
			}
		}
	}
	harness.after_mutation();
	flush_eager_models(harness, state);
}

fn anchor_step(rng: &mut StdRng, harness: &Harness, state: &mut State, p: &Params) {
	let operator = rng.random_range(1..=p.operators);
	let group = rng.random_range(1..=p.groups);
	let side = rng.random_range(0u32..p.sides as u32) as u8;
	let row_number = rng.random_range(1..=p.anchor_rows);
	match rng.random_range(0u32..10) {
		0..=6 => {
			let expiry = rng.random_range(1..=p.expiry_span);
			state.oracle.anchor_set(operator, group, side, row_number, expiry);
			for config in &harness.configs {
				config.store.anchor_set(
					OperatorId(operator),
					GroupId(group),
					side,
					RowNumber(row_number),
					DateTime::from_millis(expiry),
				);
			}
		}
		_ => {
			state.oracle.anchor_remove(operator, group, side, row_number);
			for config in &harness.configs {
				config.store.anchor_remove(
					OperatorId(operator),
					GroupId(group),
					side,
					RowNumber(row_number),
				);
			}
		}
	}
	harness.after_mutation();
	flush_eager_models(harness, state);
}

fn checkpoint_step(rng: &mut StdRng, harness: &Harness, state: &mut State, p: &Params) {
	let flow = rng.random_range(1..=p.flows);
	match rng.random_range(0u32..10) {
		0..=7 => {
			let version = rng.random_range(1..=500u64);
			state.oracle.checkpoint_set(flow, version);
			for config in &harness.configs {
				config.store.checkpoint_set(FlowId(flow), CommitVersion(version));
			}
			for model in &mut state.models {
				model.set(flow, version);
			}
		}
		_ => {
			state.oracle.checkpoint_delete(flow);
			for config in &harness.configs {
				config.store.checkpoint_delete(FlowId(flow));
			}
			for model in &mut state.models {
				model.delete(flow);
			}
		}
	}
	harness.after_mutation();
	flush_eager_models(harness, state);
}

fn drop_step(rng: &mut StdRng, harness: &Harness, state: &mut State, p: &Params) {
	let operator = rng.random_range(1..=p.operators);
	match rng.random_range(0u32..3) {
		0 => {
			state.oracle.drop_operator_state(operator);
			for config in &harness.configs {
				config.store.drop_operator_state(OperatorId(operator));
			}
		}
		1 => {
			state.oracle.anchors_drop_operator(operator);
			for config in &harness.configs {
				config.store.anchors_drop_operator(OperatorId(operator));
			}
		}
		_ => {
			let group = rng.random_range(1..=p.groups);
			state.oracle.anchors_remove_group(operator, group);
			for config in &harness.configs {
				config.store.anchors_remove_group(OperatorId(operator), GroupId(group));
			}
		}
	}
	harness.after_mutation();
	flush_eager_models(harness, state);
}

fn read_step(rng: &mut StdRng, harness: &Harness, state: &State, p: &Params, step: u32) {
	let operator = rng.random_range(1..=p.operators);
	match rng.random_range(0u32..9) {
		0 => {
			let key_bytes = random_key(rng, p);
			check_get(&harness.configs, &state.oracle, operator, &key_bytes, step);
		}
		1 => {
			let key_bytes = random_key(rng, p);
			check_contains(&harness.configs, &state.oracle, operator, &key_bytes, step);
		}
		2 | 3 => {
			let (start, end) = random_bounds(rng, p);
			let limit = rng.random_range(1..=p.max_batch);
			check_range(&harness.configs, &state.oracle, operator, &start, &end, limit, step);
		}
		4 => {
			let limit = rng.random_range(1..=p.max_batch);
			check_drain(&harness.configs, &state.oracle, operator, limit, step);
		}
		5 => {
			let group = rng.random_range(1..=p.groups);
			let side = rng.random_range(0u32..p.sides as u32) as u8;
			let row_number = rng.random_range(1..=p.anchor_rows);
			check_anchor_get(&harness.configs, &state.oracle, operator, group, side, row_number, step);
		}
		6 => {
			let group = rng.random_range(1..=p.groups);
			let limit = rng.random_range(1..=p.max_limit);
			let due = match rng.random_range(0u32..2) {
				0 => None,
				_ => Some(rng.random_range(1..=p.expiry_span)),
			};
			check_anchor_scan(&harness.configs, &state.oracle, operator, group, due, limit, step);
		}
		7 => {
			check_census(&harness.configs, &state.oracle, step);
			check_bytes(&harness.configs, &state.oracle, operator, step);
		}
		_ => {
			check_checkpoint_entries(&harness.configs, &state.oracle, p.flows, step);
			check_floors(&harness.configs, state, step);
		}
	}
}

/// Every logical surface at once. Reused by the snapshot and restart scenarios, which hold a single store
/// against the same model rather than three against each other.
pub fn verify(configs: &[Config], oracle: &Oracle, p: &Params, step: u32) {
	check_census(configs, oracle, step);
	check_anchor_census(configs, oracle, step);
	check_checkpoint_entries(configs, oracle, p.flows, step);
	for operator in 1..=p.operators {
		check_bytes(configs, oracle, operator, step);
		check_drain(configs, oracle, operator, 1, step);
		check_drain(configs, oracle, operator, p.max_batch, step);
		for group in 1..=p.groups {
			check_anchor_scan(configs, oracle, operator, group, None, u64::MAX, step);
			check_anchor_scan(configs, oracle, operator, group, Some(p.expiry_span / 2), u64::MAX, step);
		}
	}
	check_total_bytes(configs, oracle, step);
}

/// The end-of-run sweep: every operator, every group and every accounting surface, so a divergence the sampled
/// reads happened to miss still fails the run. The second pass runs with every buffer drained, which is the only
/// state in which the layered census and byte counts are held to the model.
fn sweep(harness: &Harness, state: &mut State, p: &Params, step: u32) {
	verify(&harness.configs, &state.oracle, p, step);
	check_floors(&harness.configs, state, step);

	harness.flush_all();
	for model in &mut state.models {
		model.flush();
	}
	verify(&harness.configs, &state.oracle, p, step);
	check_floors(&harness.configs, state, step);
}

pub fn check_get(configs: &[Config], oracle: &Oracle, operator: u64, key: &EncodedKey, step: u32) {
	let expected = oracle.get(operator, key.as_slice());
	for config in configs {
		let got = config.store.get(OperatorId(operator), key).map(|row| row.body().to_vec());
		assert_eq!(
			got,
			expected,
			"GET mismatch: config={} step={step} operator={operator} key={:?} store={got:?} oracle={expected:?}",
			config.name,
			key.as_slice()
		);
	}
}

pub fn check_contains(configs: &[Config], oracle: &Oracle, operator: u64, key: &EncodedKey, step: u32) {
	let expected = oracle.contains(operator, key.as_slice());
	for config in configs {
		let got = config.store.contains(OperatorId(operator), key);
		assert_eq!(
			got,
			expected,
			"CONTAINS mismatch: config={} step={step} operator={operator} key={:?} store={got} oracle={expected}",
			config.name,
			key.as_slice()
		);
	}
}

pub fn check_range(
	configs: &[Config],
	oracle: &Oracle,
	operator: u64,
	start: &Bound<EncodedKey>,
	end: &Bound<EncodedKey>,
	limit: u64,
	step: u32,
) {
	let all = oracle.range(operator, &to_bytes(start), &to_bytes(end));
	let expected_more = all.len() as u64 > limit;
	let expected: Vec<(Vec<u8>, Vec<u8>)> = all.into_iter().take(limit as usize).collect();
	for config in configs {
		let batch = config.store.range_batch(
			OperatorId(operator),
			EncodedKeyRange::new(start.clone(), end.clone()),
			limit,
		);
		let got: Vec<(Vec<u8>, Vec<u8>)> =
			batch.items.iter().map(|(key, row)| (key.to_vec(), row.body().to_vec())).collect();
		assert_eq!(
			got,
			expected,
			"RANGE mismatch: config={} step={step} operator={operator} limit={limit} start={:?} end={:?} (store {} rows vs oracle {})",
			config.name,
			start,
			end,
			got.len(),
			expected.len()
		);
		assert_eq!(
			batch.has_more, expected_more,
			"RANGE has_more mismatch: config={} step={step} operator={operator} limit={limit} store={} oracle={expected_more}",
			config.name, batch.has_more
		);
	}
}

/// Pages the whole operator out one batch at a time. A page boundary is where the buffer/sqlite merge has to
/// resume from an excluded key, so a row lost or repeated there is invisible to a single-page read.
pub fn check_drain(configs: &[Config], oracle: &Oracle, operator: u64, limit: u64, step: u32) {
	let expected = oracle.range(operator, &Bound::Unbounded, &Bound::Unbounded);
	for config in configs {
		let mut start: Bound<EncodedKey> = Bound::Unbounded;
		let mut drained: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let batch = config.store.range_batch(
				OperatorId(operator),
				EncodedKeyRange::new(start.clone(), Bound::Unbounded),
				limit,
			);
			for (key, row) in &batch.items {
				drained.push((key.to_vec(), row.body().to_vec()));
			}
			match batch.items.last() {
				Some((key, _)) => start = Bound::Excluded(key.clone()),
				None => break,
			}
			if !batch.has_more {
				break;
			}
			assert!(
				drained.len() <= expected.len() + 1,
				"DRAIN overran: config={} step={step} operator={operator} limit={limit} produced {} rows for an oracle of {}",
				config.name,
				drained.len(),
				expected.len()
			);
		}
		assert_eq!(
			drained,
			expected,
			"DRAIN mismatch: config={} step={step} operator={operator} limit={limit} (store {} rows vs oracle {})",
			config.name,
			drained.len(),
			expected.len()
		);
	}
}

pub fn check_anchor_get(
	configs: &[Config],
	oracle: &Oracle,
	operator: u64,
	group: u64,
	side: u8,
	row_number: u64,
	step: u32,
) {
	let expected = oracle.anchor_get(operator, group, side, row_number);
	for config in configs {
		let got = config
			.store
			.anchor_get(OperatorId(operator), GroupId(group), side, RowNumber(row_number))
			.map(|at| at.to_millis());
		assert_eq!(
			got, expected,
			"ANCHOR_GET mismatch: config={} step={step} operator={operator} group={group} side={side} row={row_number} store={got:?} oracle={expected:?}",
			config.name
		);
	}
}

/// Expiry ties are broken differently by each tier, so the contract checked here is the one that is actually
/// specified: ascending by expiry, exactly the `limit` smallest eligible expiries, and every returned slot a real
/// eligible anchor carrying its own expiry.
pub fn check_anchor_scan(
	configs: &[Config],
	oracle: &Oracle,
	operator: u64,
	group: u64,
	due: Option<u64>,
	limit: u64,
	step: u32,
) {
	let eligible = oracle.eligible_anchors(operator, group, due);
	let want: Vec<u64> = eligible.iter().take(limit as usize).map(|row| row.expiry).collect();
	for config in configs {
		let got = match due {
			Some(at) => config.store.anchors_due(
				OperatorId(operator),
				GroupId(group),
				DateTime::from_millis(at),
				limit,
			),
			None => config.store.anchors_by_expiry(OperatorId(operator), GroupId(group), limit),
		};
		let have: Vec<u64> = got.iter().map(|anchor| anchor.expiry.to_millis()).collect();
		assert_eq!(
			have, want,
			"ANCHOR_SCAN expiry sequence mismatch: config={} step={step} operator={operator} group={group} due={due:?} limit={limit} store={have:?} oracle={want:?}",
			config.name
		);

		let mut slots: Vec<(u8, u64)> = got.iter().map(|a| (a.side, a.row_number.0)).collect();
		let total = slots.len();
		slots.sort_unstable();
		slots.dedup();
		assert_eq!(
			slots.len(),
			total,
			"ANCHOR_SCAN returned a slot twice: config={} step={step} operator={operator} group={group} due={due:?} limit={limit}",
			config.name
		);
		for anchor in &got {
			assert!(
				eligible.contains(&AnchorRow {
					expiry: anchor.expiry.to_millis(),
					side: anchor.side,
					row_number: anchor.row_number.0,
				}),
				"ANCHOR_SCAN returned a slot the oracle does not hold at that expiry: config={} step={step} operator={operator} group={group} due={due:?} anchor={anchor:?}",
				config.name
			);
		}

		if limit as usize >= eligible.len() {
			let mut got_rows: Vec<(u64, u8, u64)> =
				got.iter().map(|a| (a.expiry.to_millis(), a.side, a.row_number.0)).collect();
			let mut want_rows: Vec<(u64, u8, u64)> =
				eligible.iter().map(|r| (r.expiry, r.side, r.row_number)).collect();
			got_rows.sort_unstable();
			want_rows.sort_unstable();
			assert_eq!(
				got_rows, want_rows,
				"ANCHOR_SCAN set mismatch under an unbounded limit: config={} step={step} operator={operator} group={group} due={due:?}",
				config.name
			);
		}
	}
}

pub fn check_census(configs: &[Config], oracle: &Oracle, step: u32) {
	let expected = oracle.census();
	for config in configs {
		if !config.census_exact() {
			continue;
		}
		let got: Vec<CensusRow> = config
			.store
			.census()
			.into_iter()
			.map(|entry| CensusRow {
				operator: entry.operator.0,
				keyspace: entry.keyspace.0,
				keys: entry.keys,
				key_bytes: entry.key_bytes.as_bytes(),
				value_bytes: entry.value_bytes.as_bytes(),
			})
			.collect();
		assert_eq!(
			got, expected,
			"CENSUS mismatch: config={} step={step} store={got:?} oracle={expected:?}",
			config.name
		);
	}
}

pub fn check_anchor_census(configs: &[Config], oracle: &Oracle, step: u32) {
	let expected = oracle.anchor_census();
	for config in configs {
		if !config.census_exact() {
			continue;
		}
		let got: Vec<(u64, u64)> =
			config.store.anchor_census().into_iter().map(|entry| (entry.operator.0, entry.keys)).collect();
		assert_eq!(
			got, expected,
			"ANCHOR_CENSUS mismatch: config={} step={step} store={got:?} oracle={expected:?}",
			config.name
		);
	}
}

pub fn check_bytes(configs: &[Config], oracle: &Oracle, operator: u64, step: u32) {
	let expected = oracle.bytes(operator);
	for config in configs {
		if !config.bytes_exact() {
			continue;
		}
		let got = config.store.bytes(OperatorId(operator)).as_bytes();
		assert_eq!(
			got, expected,
			"BYTES mismatch: config={} step={step} operator={operator} store={got} oracle={expected}",
			config.name
		);
	}
}

pub fn check_total_bytes(configs: &[Config], oracle: &Oracle, step: u32) {
	let expected = oracle.total_bytes();
	for config in configs {
		if !config.bytes_exact() {
			continue;
		}
		let got = config.store.total_bytes().as_bytes();
		assert_eq!(
			got, expected,
			"TOTAL_BYTES mismatch: config={} step={step} store={got} oracle={expected}",
			config.name
		);
	}
}

pub fn check_checkpoint_entries(configs: &[Config], oracle: &Oracle, flows: u64, step: u32) {
	let expected_list = oracle.checkpoint_list();
	for config in configs {
		for flow in 1..=flows {
			let expected = oracle.checkpoint_get(flow);
			let got = config.store.checkpoint_get(FlowId(flow)).map(|version| version.0);
			assert_eq!(
				got, expected,
				"CKPT_GET mismatch: config={} step={step} flow={flow} store={got:?} oracle={expected:?}",
				config.name
			);
		}
		let got_list: Vec<u64> = config.store.checkpoint_list().into_iter().map(|flow| flow.0).collect();
		assert_eq!(
			got_list, expected_list,
			"CKPT_LIST mismatch: config={} step={step} store={got_list:?} oracle={expected_list:?}",
			config.name
		);
	}
}

fn check_floors(configs: &[Config], state: &State, step: u32) {
	for (index, config) in configs.iter().enumerate() {
		let expected = state.models[index].floor();
		let got = config.store.checkpoint_floor().map(|version| version.0);
		assert_eq!(
			got, expected,
			"CKPT_FLOOR mismatch: config={} step={step} store={got:?} model={expected:?} (the floor may never run ahead of what a restart would restore)",
			config.name
		);
	}
}

fn flush_eager_models(harness: &Harness, state: &mut State) {
	for (index, config) in harness.configs.iter().enumerate() {
		if config.eager {
			state.models[index].flush();
		}
	}
}

fn random_key(rng: &mut StdRng, p: &Params) -> EncodedKey {
	let group = rng.random_range(1..=p.groups);
	let keyspace = KEYSPACES[rng.random_range(0u32..p.keyspaces as u32) as usize];
	let suffix = rng.random_range(1..=p.suffixes);
	key(group, keyspace, suffix)
}

fn random_bounds(rng: &mut StdRng, p: &Params) -> (Bound<EncodedKey>, Bound<EncodedKey>) {
	let a = random_key(rng, p);
	let b = random_key(rng, p);
	let (low, high) = match a.as_slice() <= b.as_slice() {
		true => (a, b),
		false => (b, a),
	};
	let start = match rng.random_range(0u32..3) {
		0 => Bound::Included(low),
		1 => Bound::Excluded(low),
		_ => Bound::Unbounded,
	};
	let end = match rng.random_range(0u32..3) {
		0 => Bound::Included(high),
		1 => Bound::Excluded(high),
		_ => Bound::Unbounded,
	};
	(start, end)
}

fn to_bytes(bound: &Bound<EncodedKey>) -> Bound<Vec<u8>> {
	match bound {
		Bound::Included(key) => Bound::Included(key.to_vec()),
		Bound::Excluded(key) => Bound::Excluded(key.to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

type BatchArgs = (Vec<OperatorWrite>, Vec<(FlowId, CommitVersion)>, Vec<FlowId>);

fn random_batch(rng: &mut StdRng, state: &mut State, p: &Params, step: u32) -> BatchArgs {
	let mut writes = Vec::new();
	let mut checkpoints = Vec::new();
	let mut deletes = Vec::new();
	let count = rng.random_range(1..=p.max_writes);
	for _ in 0..count {
		let operator = rng.random_range(1..=p.operators);
		match rng.random_range(0u32..10) {
			0..=3 => {
				let key_bytes = random_key(rng, p);
				let value = row(operator, step as u64, step);
				state.oracle.set(operator, key_bytes.as_slice(), value.clone());
				writes.push(OperatorWrite::Set {
					operator: OperatorId(operator),
					key: key_bytes,
					row: value,
				});
			}
			4..=5 => {
				let key_bytes = random_key(rng, p);
				state.oracle.remove(operator, key_bytes.as_slice());
				writes.push(OperatorWrite::Remove {
					operator: OperatorId(operator),
					key: key_bytes,
					pre_value_bytes: None,
				});
			}
			6..=8 => {
				let group = rng.random_range(1..=p.groups);
				let side = rng.random_range(0u32..p.sides as u32) as u8;
				let row_number = rng.random_range(1..=p.anchor_rows);
				let expiry = rng.random_range(1..=p.expiry_span);
				state.oracle.anchor_set(operator, group, side, row_number, expiry);
				writes.push(OperatorWrite::AnchorSet {
					operator: OperatorId(operator),
					group: GroupId(group),
					side,
					row_num: RowNumber(row_number),
					expiry: DateTime::from_millis(expiry),
				});
			}
			_ => {
				let group = rng.random_range(1..=p.groups);
				let side = rng.random_range(0u32..p.sides as u32) as u8;
				let row_number = rng.random_range(1..=p.anchor_rows);
				state.oracle.anchor_remove(operator, group, side, row_number);
				writes.push(OperatorWrite::AnchorRemove {
					operator: OperatorId(operator),
					group: GroupId(group),
					side,
					run_num: RowNumber(row_number),
				});
			}
		}
	}

	if rng.random_range(0u32..2) == 0 {
		let flow = rng.random_range(1..=p.flows);
		let version = rng.random_range(1..=500u64);
		state.oracle.checkpoint_set(flow, version);
		checkpoints.push((FlowId(flow), CommitVersion(version)));
	}
	if rng.random_range(0u32..4) == 0 {
		let flow = rng.random_range(1..=p.flows);
		state.oracle.checkpoint_delete(flow);
		deletes.push(FlowId(flow));
	}

	(writes, checkpoints, deletes)
}

/// Seed-derived configuration. The pinned sweeps stay comparable across commits; this one is what explores the
/// parameter space, so a failure reports the RESOLVED parameters rather than the master seed.
pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let min_steps = rng.random_range(60..=140u32);
	let params = Params {
		operators: pick(&mut rng, &[1u64, 2, 3, 5]),
		groups: pick(&mut rng, &[1u64, 2, 4]),
		keyspaces: rng.random_range(1..=4u64),
		suffixes: pick(&mut rng, &[8u64, 24, 64, 160]),
		flows: rng.random_range(1..=6u64),
		anchor_rows: pick(&mut rng, &[4u64, 12, 40]),
		sides: rng.random_range(1..=2u32) as u8,
		expiry_span: pick(&mut rng, &[4u64, 16, 64]),
		min_steps,
		max_steps: min_steps + rng.random_range(40..=140u32),
		write_pct: rng.random_range(20..=40u32),
		anchor_pct: rng.random_range(10..=25u32),
		checkpoint_pct: rng.random_range(4..=12u32),
		flush_pct: rng.random_range(5..=25u32),
		drop_pct: rng.random_range(1..=6u32),
		max_writes: rng.random_range(1..=8u64),
		max_batch: rng.random_range(1..=24u64),
		max_limit: rng.random_range(1..=16u64),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("operator_store_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
