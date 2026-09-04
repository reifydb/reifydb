// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::HandlerId,
	key::{
		catalog::{HandlerKey, VariantHandlerKey},
		namespace::NamespaceHandlerKey,
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_handler(txn: &mut AdminTransaction, id: HandlerId) -> Result<()> {
		let Some(handler) = Self::find_handler(&mut Transaction::Admin(&mut *txn), id)? else {
			return Ok(());
		};

		txn.remove(&VariantHandlerKey::new(
			handler.namespace,
			handler.variant.sumtype_id,
			handler.variant.variant_tag,
			id,
		))?;

		txn.remove(&NamespaceHandlerKey::new(handler.namespace, id))?;

		txn.remove(&HandlerKey::new(id))?;

		Ok(())
	}
}
