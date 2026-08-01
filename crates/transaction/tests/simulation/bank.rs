// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use rand::{RngExt, SeedableRng, rngs::StdRng};

use super::simulator::{
	executor::Executor,
	invariant::{FinalStateConsistency, Invariant},
	schedule::{Op, Schedule, Step, TxId},
};

#[test]
fn test_random_bank_transfers() {
	for seed in 0..500 {
		let (schedule, invariants) = bank_transfer(seed, 4, 4);
		let mut executor = Executor::new();
		let trace = executor.run(&schedule);

		for inv in &invariants {
			if let Err(v) = inv.check(&trace) {
				panic!("Invariant violation: {} | seed={} | schedule={:?}", v, seed, schedule);
			}
		}
	}
}

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
