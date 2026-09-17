// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorRetentionChangeOperations, flow::OperatorId},
	key::operator_retention::OperatorRetentionKey,
	row::OperatorRetention,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use super::encode_operator_retention;
use crate::Result;

pub fn create_operator_retention(
	txn: &mut AdminTransaction,
	operator: OperatorId,
	retention: &OperatorRetention,
) -> Result<()> {
	let value = encode_operator_retention(retention);
	txn.set(&OperatorRetentionKey::new(operator), value)?;
	txn.track_operator_retention_created(operator, retention.clone())?;
	Ok(())
}
