// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{NamespaceId, RingBufferDef, RingBufferId};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction};
use reifydb_type::{error, internal};
use tracing::instrument;

use crate::{Catalog, CatalogStore};

impl Catalog {
	#[instrument(name = "catalog::ringbuffer::find", level = "trace", skip(self, txn))]
	pub async fn find_ringbuffer<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: RingBufferId,
	) -> crate::Result<Option<RingBufferDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => CatalogStore::find_ringbuffer(cmd, id).await,
			StandardTransaction::Query(qry) => CatalogStore::find_ringbuffer(qry, id).await,
		}
	}

	#[instrument(name = "catalog::ringbuffer::find_by_name", level = "trace", skip(self, txn, name))]
	pub async fn find_ringbuffer_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<RingBufferDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				CatalogStore::find_ringbuffer_by_name(cmd, namespace, name).await
			}
			StandardTransaction::Query(qry) => {
				CatalogStore::find_ringbuffer_by_name(qry, namespace, name).await
			}
		}
	}

	#[instrument(name = "catalog::ringbuffer::get", level = "trace", skip(self, txn))]
	pub async fn get_ringbuffer<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: RingBufferId,
	) -> crate::Result<RingBufferDef> {
		self.find_ringbuffer(txn, id).await?.ok_or_else(|| {
			error!(internal!(
				"RingBuffer with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}
}
