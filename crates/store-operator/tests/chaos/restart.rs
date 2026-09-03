// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a boot after a crash sees. A store opened over a real sqlite file with its flush interval parked an hour
//! out only ever makes durable what the run explicitly flushed, so a second store opened over the same file must
//! show exactly the model as of the last flush: never a write that was still buffered, and never less than what a
//! completed flush promised.

use std::cell::Cell;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
};
use reifydb_store_operator::{
	store::OperatorStore,
	types::{DurablePre, OperatorWrite},
};
use reifydb_testing::tempdir::temp_dir;

use crate::{
	fixtures::{Config, KEYSPACES, key, row, spawner, store_at},
	oracle::Oracle,
	workload::{Params, verify},
};

pub fn drive(seed: u64, p: Params) {
	temp_dir(|dir| {
		let mut rng = StdRng::seed_from_u64(seed);
		let spawner = spawner();
		let store = store_at(&spawner, dir);

		let mut oracle = Oracle::default();
		let mut durable = Oracle::default();

		let steps = rng.random_range(p.min_steps..=p.max_steps);
		for step in 0..steps {
			let roll = rng.random_range(0u32..100);
			if roll < p.flush_pct {
				assert!(
					store.flush_pending_blocking(),
					"a healthy sqlite tier must report the flush as completed"
				);
				durable = oracle.clone();
				continue;
			}
			mutate(&mut rng, &store, &mut oracle, &p, step);
		}

		let booted = store_at(&spawner, dir);
		verify(&[config("booted", booted)], &durable, &p, steps);

		assert!(store.flush_pending_blocking(), "the tail flush must complete before the second boot");

		let booted = store_at(&spawner, dir);
		verify(&[config("booted_after_tail_flush", booted)], &oracle, &p, steps);
		Ok(())
	})
	.unwrap();
}

fn config(name: &'static str, store: OperatorStore) -> Config {
	Config {
		name,
		store,
		accounts_bytes: true,
		eager: false,
		write_through: false,
		buffer_clean: Cell::new(true),
	}
}

fn mutate(rng: &mut StdRng, store: &OperatorStore, oracle: &mut Oracle, p: &Params, step: u32) {
	let operator = rng.random_range(1..=p.operators);
	match rng.random_range(0u32..9) {
		0..=4 => {
			let group = rng.random_range(1..=p.groups);
			let keyspace = KEYSPACES[rng.random_range(0u32..p.keyspaces as u32) as usize];
			let suffix = rng.random_range(1..=p.suffixes);
			let key_bytes = key(group, keyspace, suffix);
			let value = row(operator, suffix, step);

			let pre = oracle.value_bytes(operator, key_bytes.as_slice());
			oracle.set(operator, key_bytes.as_slice(), value.clone());
			store.apply_batch(&[match pre {
				Some(pre_value_bytes) => OperatorWrite::Replace {
					operator: OperatorId(operator),
					key: key_bytes,
					pre_value_bytes,
					post: value,
				},
				None => OperatorWrite::Insert {
					operator: OperatorId(operator),
					key: key_bytes,
					post: value,
				},
			}]);
		}
		5..=6 => {
			let group = rng.random_range(1..=p.groups);
			let keyspace = KEYSPACES[rng.random_range(0u32..p.keyspaces as u32) as usize];
			let suffix = rng.random_range(1..=p.suffixes);
			let key_bytes = key(group, keyspace, suffix);

			let pre = match oracle.value_bytes(operator, key_bytes.as_slice()) {
				Some(pre_value_bytes) => DurablePre::Present(pre_value_bytes),
				None => DurablePre::Absent,
			};
			oracle.remove(operator, key_bytes.as_slice());
			store.apply_batch(&[OperatorWrite::Remove {
				operator: OperatorId(operator),
				key: key_bytes,
				pre,
			}]);
		}
		7 => {
			let flow = rng.random_range(1..=p.flows);
			let version = rng.random_range(1..=500u64);

			oracle.checkpoint_set(flow, version);
			store.checkpoint_set(FlowId(flow), CommitVersion(version));
		}
		_ => {
			oracle.drop_operator_state(operator);
			store.drop_operator_state(OperatorId(operator));
		}
	}
}
