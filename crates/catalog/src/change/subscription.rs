// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		id::{PrimaryKeyId, SubscriptionId},
		subscription::Subscription,
	},
	key::{EncodableKey, kind::KeyKind, subscription::SubscriptionKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::subscription::schema::subscription::{self, ACKNOWLEDGED_VERSION, ID, PRIMARY_KEY},
};

pub(super) struct SubscriptionApplier;

impl CatalogChangeApplier for SubscriptionApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let sub = decode_subscription(row, &catalog.materialized, txn.version());
		catalog.materialized.set_subscription(sub.id, txn.version(), Some(sub));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = SubscriptionKey::decode(key).map(|k| k.subscription).ok_or(
			CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::Subscription,
			},
		)?;
		catalog.materialized.set_subscription(id, txn.version(), None);
		Ok(())
	}
}

use crate::materialized::MaterializedCatalog;

fn decode_subscription(row: &EncodedRow, materialized: &MaterializedCatalog, version: CommitVersion) -> Subscription {
	let id = SubscriptionId(subscription::SCHEMA.get_u64(row, ID));
	let acknowledged_version = CommitVersion(subscription::SCHEMA.get_u64(row, ACKNOWLEDGED_VERSION));
	let pk_raw = subscription::SCHEMA.get_u64(row, PRIMARY_KEY);
	let primary_key = if pk_raw > 0 {
		materialized.find_primary_key_at(PrimaryKeyId(pk_raw), version)
	} else {
		None
	};

	Subscription {
		id,
		columns: vec![],
		primary_key,
		acknowledged_version,
	}
}
