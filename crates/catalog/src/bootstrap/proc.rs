// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{event::EventBus, interface::catalog::id::NamespaceId};
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
	)?;

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRIC_PROC,
		"system::metric::proc",
		"proc",
		NamespaceId::SYSTEM_METRIC,
	)?;

	for (id, path, local_name, parent) in PROC_NAMESPACES {
		ensure_namespace(&catalog_api, &mut admin, id, path, local_name, parent)?;
	}

	admin.commit()?;
	Ok(())
}

const PROC_NAMESPACES: [(NamespaceId, &str, &str, NamespaceId); 9] = [
	(
		NamespaceId::SYSTEM_METRIC_PROC_PROCESS,
		"system::metric::proc::process",
		"process",
		NamespaceId::SYSTEM_METRIC_PROC,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_PROCESS_IO,
		"system::metric::proc::process::io",
		"io",
		NamespaceId::SYSTEM_METRIC_PROC_PROCESS,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_PROCESS_MEMORY,
		"system::metric::proc::process::memory",
		"memory",
		NamespaceId::SYSTEM_METRIC_PROC_PROCESS,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_PROCESS_SCHED,
		"system::metric::proc::process::sched",
		"sched",
		NamespaceId::SYSTEM_METRIC_PROC_PROCESS,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP,
		"system::metric::proc::cgroup",
		"cgroup",
		NamespaceId::SYSTEM_METRIC_PROC,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP_IO,
		"system::metric::proc::cgroup::io",
		"io",
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP_MEMORY,
		"system::metric::proc::cgroup::memory",
		"memory",
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP_CPU,
		"system::metric::proc::cgroup::cpu",
		"cpu",
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP,
	),
	(
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP_PRESSURE,
		"system::metric::proc::cgroup::pressure",
		"pressure",
		NamespaceId::SYSTEM_METRIC_PROC_CGROUP,
	),
];
