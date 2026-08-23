// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A reopened store must show exactly the blocks the last flush wrote: never a still-buffered record, never a truncated
//! prefix.

use std::path::Path;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::common::CommitVersion;
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_sqlite::SqliteConfig;
use reifydb_store_cdc::{
	storage::{CdcStorage, Cutoff},
	tier::persistent::CdcPersistentTier,
};
use reifydb_testing::tempdir::temp_dir;

use crate::{
	fixtures::{CUT_LARGE, Config, config, flush, spawner},
	oracle::{Oracle, TtlCutoff},
	workload::{Generator, Params, build_record, check_bounds, check_commit_metrics, verify},
};

pub fn drive(seed: u64, p: Params) {
	temp_dir(|dir| {
		let mut rng = StdRng::seed_from_u64(seed);
		let spawner = spawner();
		let mut live = boot(&spawner, dir, "live", Oracle::new(CUT_LARGE.as_bytes()));
		let mut generator = Generator::new();

		let steps = rng.random_range(p.min_steps..=p.max_steps);
		for step in 0..steps {
			let roll = rng.random_range(0u32..100);
			if roll < p.flush_pct {
				flush(&mut live);
				continue;
			}
			if roll < p.flush_pct + p.drop_pct {
				let cutoff = CommitVersion(rng.random_range(0..=generator.ceiling().saturating_add(2)));
				let limit = rng.random_range(0..=p.max_limit as u64) as usize;
				let expected = live.oracle.drop_before(TtlCutoff::Version(cutoff.0), limit);
				let got = live.store.drop_before(Cutoff::Version(cutoff), limit).unwrap();
				assert_eq!(
					(got.count.as_u64(), got.more_remaining),
					(expected.count, expected.more_remaining),
					"DROP mismatch before a boot: step={step} cutoff={} limit={limit}",
					cutoff.0
				);
				continue;
			}
			write(&mut rng, &mut live, &mut generator, &p, step);
			live.oracle.check_invariants(live.name, step);
			check_bounds(&live, step);
			check_commit_metrics(&live, step);
		}

		// A crash leaves whatever the commit tier held unsealed, so the boot must land on the model with every
		// buffered record removed.
		let mut durable = live.oracle.clone();
		durable.reopen();
		let crashed = boot(&spawner, dir, "booted", durable);
		verify(&crashed, &p, steps);
		crashed.store.shutdown();

		flush(&mut live);
		let sealed = live.oracle.clone();
		live.store.shutdown();

		let clean = boot(&spawner, dir, "booted_after_tail_flush", sealed);
		verify(&clean, &p, steps);
		clean.store.shutdown();
		Ok(())
	})
	.unwrap();
}

fn boot(spawner: &ActorSpawner, dir: &Path, name: &'static str, oracle: Oracle) -> Config {
	let tier = (CdcPersistentTier::sqlite(SqliteConfig::new(dir)), None);
	let mut config = config(name, spawner, tier, None, CUT_LARGE);
	config.oracle = oracle;
	config
}

fn write(rng: &mut StdRng, config: &mut Config, generator: &mut Generator, p: &Params, step: u32) {
	let (version, cdc, row) = build_record(rng, generator, p);
	assert!(config.oracle.write(version, row), "WRITE rejected by the model: step={step} version={version}");
	config.store
		.write(&cdc)
		.unwrap_or_else(|err| panic!("WRITE rejected by the store: step={step} version={version} err={err:?}"));
	if config.oracle.should_cut() {
		flush(config);
	}
}
