// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::HandlerId,
	key::{handler::HandlerKey, namespace_handler::NamespaceHandlerKey, variant_handler::VariantHandlerKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_handler(txn: &mut AdminTransaction, id: HandlerId) -> Result<()> {
		let Some(handler) = Self::find_handler(&mut Transaction::Admin(&mut *txn), id)? else {
			return Ok(());
		};

		txn.remove(&VariantHandlerKey::encoded(
			handler.namespace,
			handler.variant.sumtype_id,
			handler.variant.variant_tag,
			id,
		))?;

		txn.remove(&NamespaceHandlerKey::encoded(handler.namespace, id))?;

		txn.remove(&HandlerKey::encoded(id))?;

		Ok(())
	}
}
