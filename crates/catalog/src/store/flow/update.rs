// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::{FlowId, FlowStatus},
	key::flow::FlowKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::flow::shape::flow};

impl CatalogStore {
	pub(crate) fn update_flow_name(txn: &mut AdminTransaction, flow_id: FlowId, new_name: String) -> Result<()> {
		let flow = Self::get_flow(&mut Transaction::Admin(&mut *txn), flow_id)?;

		let mut row = flow::allocate();
		flow::set_id(&mut row, flow_id.0);
		flow::set_namespace(&mut row, flow.namespace.0);
		flow::set_name(&mut row, &new_name);
		flow::set_status(&mut row, flow.status.to_u8());
		if let Some(reason) = flow.status.reason() {
			flow::set_reason(&mut row, reason);
		}

		txn.set(vec![(FlowKey::new(flow_id).into(), row.freeze().into())])?;

		Ok(())
	}

	pub(crate) fn poison_flow(txn: &mut AdminTransaction, flow_id: FlowId, reason: String) -> Result<()> {
		let Some(flow) = Self::find_flow(&mut Transaction::Admin(&mut *txn), flow_id)? else {
			return Ok(());
		};
		if flow.status != FlowStatus::Active {
			return Ok(());
		}
		let status = FlowStatus::Poisoned {
			reason,
		};

		let mut row = flow::allocate();
		flow::set_id(&mut row, flow_id.0);
		flow::set_namespace(&mut row, flow.namespace.0);
		flow::set_name(&mut row, &flow.name);
		flow::set_status(&mut row, status.to_u8());
		if let Some(reason) = status.reason() {
			flow::set_reason(&mut row, reason);
		}

		txn.set(vec![(FlowKey::new(flow_id).into(), row.freeze().into())])?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowStatus;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::test_utils::ensure_test_flow;

	#[test]
	fn test_update_flow_name() {
		let mut txn = create_test_admin_transaction();
		let flow = ensure_test_flow(&mut txn);

		CatalogStore::update_flow_name(&mut txn, flow.id, "new_flow_name".to_string()).unwrap();

		let updated = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(updated.name, "new_flow_name");
		assert_eq!(updated.namespace, flow.namespace);
		assert_eq!(updated.status, flow.status);
	}

	#[test]
	fn test_poison_flow() {
		let mut txn = create_test_admin_transaction();
		let flow = ensure_test_flow(&mut txn);

		// The reason must survive the row round trip, and a second poison must never replace the root cause.
		assert_eq!(flow.status, FlowStatus::Active);

		CatalogStore::poison_flow(&mut txn, flow.id, "v 13 refused".to_string()).unwrap();
		let updated = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(
			updated.status,
			FlowStatus::Poisoned {
				reason: "v 13 refused".to_string()
			}
		);

		CatalogStore::poison_flow(&mut txn, flow.id, "upstream t::a is poisoned".to_string()).unwrap();
		let updated = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(
			updated.status,
			FlowStatus::Poisoned {
				reason: "v 13 refused".to_string()
			}
		);
	}
}
