// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::HandlerId,
	key::{handler::HandlerKey, namespace_handler::NamespaceHandlerKey, variant_handler::VariantHandlerKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_handler(txn: &mut AdminTransaction, id: HandlerId) -> Result<()> {
		// Fetch the handler first to get namespace and variant info for secondary index cleanup
		let Some(handler) = Self::find_handler(&mut Transaction::Admin(&mut *txn), id)? else {
			return Ok(());
		};

		// Remove variant index row
		txn.remove(&VariantHandlerKey::encoded(
			handler.namespace,
			handler.variant.sumtype_id,
			handler.variant.variant_tag,
			id,
		))?;

		// Remove namespace index row
		txn.remove(&NamespaceHandlerKey::encoded(handler.namespace, id))?;

		// Remove primary row
		txn.remove(&HandlerKey::encoded(id))?;

		Ok(())
	}
}
