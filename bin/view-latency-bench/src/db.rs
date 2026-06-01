// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::path::PathBuf;

use reifydb::{
	Database, Params, Value, WithSubsystem, core::interface::catalog::config::ConfigKey, embedded,
	value::value::duration::Duration,
};

use crate::workload::{self, Kind, Workload};

pub struct Config {
	pub flow_tick_ms: u64,
	pub threads: u16,
}

pub fn build(workload: Workload, kind: Kind, n: usize, operators_dir: Option<PathBuf>, config: &Config) -> Database {
	let tick = Value::Duration(Duration::from_milliseconds(config.flow_tick_ms as i64).unwrap());
	let threads = Value::Uint2(config.threads);
	let configs = [
		(ConfigKey::FlowTick, tick),
		(ConfigKey::ThreadsCommit, threads.clone()),
		(ConfigKey::ThreadsSystem, threads.clone()),
		(ConfigKey::FlowWorkerThreads, threads),
	];

	let db = match operators_dir {
		Some(dir) => embedded::memory().with_configs(configs).with_flow(move |f| f.operators_dir(dir)).build(),
		None => embedded::memory().with_configs(configs).with_flow(|f| f).build(),
	}
	.expect("failed to build embedded database");

	setup(&db, workload, kind, n);
	db
}

fn admin(db: &Database, rql: &str) {
	db.admin_as_root(rql, Params::None).unwrap_or_else(|e| panic!("admin failed: {e:?}\nrql: {rql}"));
}

fn setup(db: &Database, workload: Workload, kind: Kind, n: usize) {
	admin(db, "create namespace bench");
	for stmt in workload::schema(workload) {
		admin(db, &stmt);
	}
	admin(db, &workload::probe_view(kind));
	for i in 1..n {
		admin(db, &workload::workload_view(workload, kind, i));
	}
}
