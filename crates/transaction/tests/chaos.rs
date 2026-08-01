// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

use std::collections::BTreeMap;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_testing_chaos::fuzz::{run_reported, split};
use reifydb_testing_macro::chaos_test;

#[allow(dead_code)]
#[path = "simulation/simulator/mod.rs"]
mod simulator;

use simulator::{
	executor::Executor,
	invariant::{
		FinalStateConsistency, Invariant, NoDirtyReads, NoLostUpdates, ReadYourOwnWrites, SnapshotConsistency,
	},
	schedule::{Op, Schedule, Step, TxId},
};

fn run_and_assert(seed: u64, schedule: Schedule, invariants: Vec<Box<dyn Invariant>>) {
	let mut executor = Executor::new();
	let trace = executor.run(&schedule);
	for inv in &invariants {
		if let Err(v) = inv.check(&trace) {
			panic!("Invariant violation: {} | seed={} | schedule={:?}", v, seed, schedule);
		}
	}
}

chaos_test!(bank_transfers_chaos, |seed| {
	// Replaying committed transfers in commit order catches a lost update or an out-of-order apply.
	let (schedule, invariants) = bank_transfer(seed, 4, 4);
	run_and_assert(seed, schedule, invariants);
});

chaos_test!(counter_increments_chaos, |seed| {
	// A missed write-write conflict between concurrent increments shows up as a lost update.
	let (schedule, invariants) = counter_increment(seed, 5);
	run_and_assert(seed, schedule, invariants);
});

chaos_test!(random_mixed_workload_chaos, |seed| {
	// The key space is deliberately small so transactions collide; the four invariants hold under
	// snapshot isolation for any schedule at all, so any seed that breaks one is a real defect.
	let (schedule, invariants) = random_mixed(seed, 6, 8);
	run_and_assert(seed, schedule, invariants);
});

const KEY_SPACE: u32 = 8;

/// How many transactions interleave and how many operations each holds.
///
/// Sweeps that pin a shape stay comparable across commits; sweeps that draw it from the seed explore
/// the dimension instead, and more transactions over the same eight keys means more conflicts.
#[derive(Clone, Debug)]
struct Shape {
	txs: u32,
	ops: u32,
}

fn random_shape(seed: u64) -> (u64, Shape) {
	let (mut rng, sequence_seed) = split(seed);
	let shape = Shape {
		txs: rng.random_range(2..=10u32),
		ops: rng.random_range(2..=12u32),
	};
	(sequence_seed, shape)
}

fn random_mixed(seed: u64, max_txs: u32, max_ops: u32) -> (Schedule, Vec<Box<dyn Invariant>>) {
	let mut rng = StdRng::seed_from_u64(seed);

	let num_txs = rng.random_range(1..=max_txs) as usize;

	let mut tx_ops: Vec<Vec<Op>> = Vec::new();
	for _ in 0..num_txs {
		let is_write = rng.random_range(0u32..10) < 7;

		let mut ops = Vec::new();
		ops.push(if is_write {
			Op::BeginCommand
		} else {
			Op::BeginQuery
		});

		let n = rng.random_range(0..=max_ops);
		for _ in 0..n {
			let key = format!("k{}", rng.random_range(0..KEY_SPACE));
			let op = if is_write {
				match rng.random_range(0u32..10) {
					0..=3 => Op::Set {
						key,
						value: format!("v{}", rng.random_range(0u32..100)),
					},
					4..=6 => Op::Get {
						key,
					},
					7..=8 => Op::Remove {
						key,
					},
					_ => Op::Scan,
				}
			} else {
				match rng.random_range(0u32..10) {
					0..=7 => Op::Get {
						key,
					},
					_ => Op::Scan,
				}
			};
			ops.push(op);
		}

		let terminal = if is_write && rng.random_range(0u32..10) < 7 {
			Op::Commit
		} else {
			Op::Rollback
		};
		ops.push(terminal);

		tx_ops.push(ops);
	}

	let mut steps = Vec::new();
	let mut cursors: Vec<usize> = vec![0; num_txs];
	let mut active: Vec<usize> = (0..num_txs).collect();
	while !active.is_empty() {
		let idx = rng.random_range(0..active.len());
		let tx_idx = active[idx];
		let cursor = cursors[tx_idx];

		if cursor < tx_ops[tx_idx].len() {
			steps.push(Step {
				tx_id: TxId(tx_idx as u32),
				op: tx_ops[tx_idx][cursor].clone(),
			});
			cursors[tx_idx] += 1;
		}

		if cursors[tx_idx] >= tx_ops[tx_idx].len() {
			active.swap_remove(idx);
		}
	}

	let schedule = Schedule {
		steps,
	};

	let invariants: Vec<Box<dyn Invariant>> = vec![
		Box::new(NoDirtyReads),
		Box::new(ReadYourOwnWrites),
		Box::new(SnapshotConsistency),
		Box::new(NoLostUpdates),
	];

	(schedule, invariants)
}

/// Read-both/write-both transfers interleaved at random; the returned invariant replays the commits
/// in order, so a lost update or an out-of-order apply fails it.
fn bank_transfer(seed: u64, num_accounts: usize, num_transfers: usize) -> (Schedule, Vec<Box<dyn Invariant>>) {
	let mut rng = StdRng::seed_from_u64(seed);
	let initial_balance: i64 = 100;

	let mut steps = Vec::new();
	steps.push(Step {
		tx_id: TxId(0),
		op: Op::BeginCommand,
	});
	for i in 0..num_accounts {
		steps.push(Step {
			tx_id: TxId(0),
			op: Op::Set {
				key: format!("account_{}", i),
				value: format!("{}", initial_balance),
			},
		});
	}
	steps.push(Step {
		tx_id: TxId(0),
		op: Op::Commit,
	});

	// transfer_writes[i] belongs to TxId(i+1); TxId(0) is the setup transaction.
	let mut transfer_writes: Vec<(String, String, String, String)> = Vec::new();
	let mut tx_ops: Vec<Vec<Op>> = Vec::new();
	for _ in 0..num_transfers {
		let from = rng.random_range(0..num_accounts);
		let mut to = rng.random_range(0..num_accounts);
		while to == from {
			to = rng.random_range(0..num_accounts);
		}
		let amount: i64 = rng.random_range(1..=20);

		let from_key = format!("account_{}", from);
		let to_key = format!("account_{}", to);
		let from_value = format!("{}", initial_balance - amount);
		let to_value = format!("{}", initial_balance + amount);

		transfer_writes.push((from_key.clone(), to_key.clone(), from_value.clone(), to_value.clone()));

		let mut ops = Vec::new();
		ops.push(Op::BeginCommand);
		ops.push(Op::Get {
			key: from_key.clone(),
		});
		ops.push(Op::Get {
			key: to_key.clone(),
		});
		ops.push(Op::Set {
			key: from_key,
			value: from_value,
		});
		ops.push(Op::Set {
			key: to_key,
			value: to_value,
		});
		ops.push(Op::Commit);

		tx_ops.push(ops);
	}

	let mut cursors: Vec<usize> = vec![0; num_transfers];
	let mut active: Vec<usize> = (0..num_transfers).collect();

	while !active.is_empty() {
		let idx = rng.random_range(0..active.len());
		let tx_idx = active[idx];
		let cursor = cursors[tx_idx];

		if cursor < tx_ops[tx_idx].len() {
			steps.push(Step {
				tx_id: TxId((tx_idx + 1) as u32),
				op: tx_ops[tx_idx][cursor].clone(),
			});
			cursors[tx_idx] += 1;
		}

		if cursors[tx_idx] >= tx_ops[tx_idx].len() {
			active.swap_remove(idx);
		}
	}

	let schedule = Schedule {
		steps,
	};

	let invariants: Vec<Box<dyn Invariant>> = vec![Box::new(FinalStateConsistency {
		name: "bank_replay_final_state".into(),
		predicate: Box::new(move |state, trace| {
			let mut expected: BTreeMap<String, String> = BTreeMap::new();
			for i in 0..num_accounts {
				expected.insert(format!("account_{}", i), format!("{}", initial_balance));
			}

			// Commit order, not schedule order, is what the final state must reflect.
			let mut committed: Vec<_> = transfer_writes
				.iter()
				.enumerate()
				.filter(|(i, _)| trace.committed.contains_key(&TxId((*i + 1) as u32)))
				.collect();
			committed.sort_by_key(|(i, _)| trace.committed[&TxId((*i + 1) as u32)]);

			for (_, (from_key, to_key, from_value, to_value)) in &committed {
				expected.insert(from_key.clone(), from_value.clone());
				expected.insert(to_key.clone(), to_value.clone());
			}

			if state != &expected {
				return Err(format!(
					"final state mismatch after replaying {} committed transfers.\n  expected: {:?}\n  actual:   {:?}",
					committed.len(),
					expected,
					state
				));
			}

			Ok(())
		}),
	})];

	(schedule, invariants)
}

/// Read-then-overwrite transactions, each writing its own id, interleaved at random; the invariants
/// pin last-writer-wins to the highest-version committer, so a missed conflict fails them.
fn counter_increment(seed: u64, num_transactions: usize) -> (Schedule, Vec<Box<dyn Invariant>>) {
	let mut rng = StdRng::seed_from_u64(seed);

	let mut steps = Vec::new();
	steps.push(Step {
		tx_id: TxId(0),
		op: Op::BeginCommand,
	});
	steps.push(Step {
		tx_id: TxId(0),
		op: Op::Set {
			key: "counter".into(),
			value: "0".into(),
		},
	});
	steps.push(Step {
		tx_id: TxId(0),
		op: Op::Commit,
	});

	let mut tx_ops: Vec<Vec<Op>> = Vec::new();
	for i in 0..num_transactions {
		let mut ops = Vec::new();
		ops.push(Op::BeginCommand);
		ops.push(Op::Get {
			key: "counter".into(),
		});
		ops.push(Op::Set {
			key: "counter".into(),
			value: format!("{}", i + 1),
		});
		ops.push(Op::Commit);
		tx_ops.push(ops);
	}

	let mut cursors: Vec<usize> = vec![0; num_transactions];
	let mut active: Vec<usize> = (0..num_transactions).collect();

	while !active.is_empty() {
		let idx = rng.random_range(0..active.len());
		let tx_idx = active[idx];
		let cursor = cursors[tx_idx];

		if cursor < tx_ops[tx_idx].len() {
			steps.push(Step {
				tx_id: TxId((tx_idx + 1) as u32),
				op: tx_ops[tx_idx][cursor].clone(),
			});
			cursors[tx_idx] += 1;
		}

		if cursors[tx_idx] >= tx_ops[tx_idx].len() {
			active.swap_remove(idx);
		}
	}

	let schedule = Schedule {
		steps,
	};

	let invariants: Vec<Box<dyn Invariant>> = vec![
		Box::new(FinalStateConsistency {
			name: "counter_last_writer_wins".into(),
			predicate: Box::new(move |state, trace| {
				let counter_val = state.get("counter").ok_or("counter key not found in final state")?;

				let last_writer: Option<TxId> = trace.committed.iter()
					.filter(|(tx_id, _)| tx_id.0 > 0) // skip setup tx
					.max_by_key(|(_, version)| *version)
					.map(|(&tx_id, _)| tx_id);

				let expected_value = match last_writer {
					Some(tx_id) => format!("{}", tx_id.0),
					None => "0".to_string(),
				};

				if counter_val != &expected_value {
					return Err(format!(
						"counter value '{}', expected '{}' (from last committed writer {:?})",
						counter_val, expected_value, last_writer
					));
				}

				Ok(())
			}),
		}),
		Box::new(NoLostUpdates),
	];

	(schedule, invariants)
}

chaos_test!(random_mixed_workload_random_shape_chaos, |seed| {
	let (sequence_seed, shape) = random_shape(seed);
	let run = shape.clone();
	run_reported("random_mixed_workload_random_shape_chaos", sequence_seed, &shape, || {
		let (schedule, invariants) = random_mixed(sequence_seed, run.txs, run.ops);
		run_and_assert(sequence_seed, schedule, invariants);
	});
});

chaos_test!(bank_transfers_random_shape_chaos, |seed| {
	let (sequence_seed, shape) = random_shape(seed);
	let run = shape.clone();
	run_reported("bank_transfers_random_shape_chaos", sequence_seed, &shape, || {
		let (schedule, invariants) = bank_transfer(sequence_seed, run.txs as usize, run.ops as usize);
		run_and_assert(sequence_seed, schedule, invariants);
	});
});

chaos_test!(counter_increments_random_shape_chaos, |seed| {
	let (sequence_seed, shape) = random_shape(seed);
	let run = shape.clone();
	run_reported("counter_increments_random_shape_chaos", sequence_seed, &shape, || {
		let (schedule, invariants) = counter_increment(sequence_seed, run.txs as usize);
		run_and_assert(sequence_seed, schedule, invariants);
	});
});
