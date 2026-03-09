// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use rand::{RngExt, SeedableRng, rngs::StdRng};

use super::simulator::{
	executor::Executor,
	invariant::{FinalStateConsistency, Invariant, NoLostUpdates},
	schedule::{Op, Schedule, Step, TxId},
};

#[test]
fn test_random_counter_increments() {
	for seed in 0..500 {
		let (schedule, invariants) = counter_increment(seed, 5);
		let mut executor = Executor::new();
		let trace = executor.run(&schedule);

		for inv in &invariants {
			if let Err(v) = inv.check(&trace) {
				panic!("Invariant violation: {} | seed={} | schedule={:?}", v, seed, schedule);
			}
		}
	}
}

fn counter_increment(seed: u64, num_transactions: usize) -> (Schedule, Vec<Box<dyn Invariant>>) {
	let mut rng = StdRng::seed_from_u64(seed);

	// Setup: initialize counter to "0"
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

	// Each transaction: read counter, write its own id
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

	// Interleave
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

				// Find the last committed increment tx (highest commit version)
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
