// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{policy::PolicyKey, policy_op::PolicyOpKey};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{
	Result,
	store::policy::{convert_policy, convert_policy_op},
};

pub(crate) fn load_policies(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = PolicyKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let policy = convert_policy(multi);
		catalog.set_policy(policy.id, version, Some(policy));
	}
	drop(stream);

	// Load policy operations
	let op_range = PolicyOpKey::full_scan();
	let mut op_stream = rx.range(op_range, 1024)?;

	while let Some(entry) = op_stream.next() {
		let multi = entry?;
		let op_def = convert_policy_op(multi);
		let policy_id = op_def.policy_id;
		if let Some(existing) = catalog.policy_operations.get(&policy_id) {
			let mut ops = existing.value().clone();
			ops.push(op_def);
			catalog.set_policy_operations(policy_id, ops);
		} else {
			catalog.set_policy_operations(policy_id, vec![op_def]);
		}
	}

	Ok(())
}
