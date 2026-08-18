// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{event::EventBus, interface::catalog::id::NamespaceId};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction, single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_value::value::identity::IdentityId;

use super::ensure_namespace;
use crate::{Result, cache::CatalogCache, catalog::Catalog};

pub fn bootstrap_proc(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
) -> Result<()> {
	let catalog_api = Catalog::new(catalog.clone());
	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)?;

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_PROC,
		"system::metrics::proc",
		"proc",
		NamespaceId::SYSTEM_METRICS,
	)?;

	for (id, path, local_name, parent) in PROC_NAMESPACES {
		ensure_namespace(&catalog_api, &mut admin, id, path, local_name, parent)?;
	}

	admin.commit()?;
	Ok(())
}

const PROC_NAMESPACES: [(NamespaceId, &str, &str, NamespaceId); 9] = [
	(
		NamespaceId::SYSTEM_METRICS_PROC_PROCESS,
		"system::metrics::proc::process",
		"process",
		NamespaceId::SYSTEM_METRICS_PROC,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_PROCESS_IO,
		"system::metrics::proc::process::io",
		"io",
		NamespaceId::SYSTEM_METRICS_PROC_PROCESS,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_PROCESS_MEMORY,
		"system::metrics::proc::process::memory",
		"memory",
		NamespaceId::SYSTEM_METRICS_PROC_PROCESS,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_PROCESS_SCHED,
		"system::metrics::proc::process::sched",
		"sched",
		NamespaceId::SYSTEM_METRICS_PROC_PROCESS,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP,
		"system::metrics::proc::cgroup",
		"cgroup",
		NamespaceId::SYSTEM_METRICS_PROC,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP_IO,
		"system::metrics::proc::cgroup::io",
		"io",
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP_MEMORY,
		"system::metrics::proc::cgroup::memory",
		"memory",
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP_CPU,
		"system::metrics::proc::cgroup::cpu",
		"cpu",
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP,
	),
	(
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP_PRESSURE,
		"system::metrics::proc::cgroup::pressure",
		"pressure",
		NamespaceId::SYSTEM_METRICS_PROC_CGROUP,
	),
];
