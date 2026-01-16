// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	id::{NamespaceId, RingBufferId},
	ringbuffer::RingBufferDef,
};
use reifydb_transaction::standard::{IntoStandardTransaction, StandardTransaction};
use reifydb_type::{error, internal};
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog};

impl Catalog {
	#[instrument(name = "catalog::ringbuffer::find", level = "trace", skip(self, txn))]
	pub fn find_ringbuffer<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: RingBufferId,
	) -> crate::Result<Option<RingBufferDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => CatalogStore::find_ringbuffer(cmd, id),
			StandardTransaction::Query(qry) => CatalogStore::find_ringbuffer(qry, id),
		}
	}

	#[instrument(name = "catalog::ringbuffer::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_ringbuffer_by_name<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<RingBufferDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				CatalogStore::find_ringbuffer_by_name(cmd, namespace, name)
			}
			StandardTransaction::Query(qry) => CatalogStore::find_ringbuffer_by_name(qry, namespace, name),
		}
	}

	#[instrument(name = "catalog::ringbuffer::get", level = "trace", skip(self, txn))]
	pub fn get_ringbuffer<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: RingBufferId,
	) -> crate::Result<RingBufferDef> {
		self.find_ringbuffer(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"RingBuffer with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}
}
