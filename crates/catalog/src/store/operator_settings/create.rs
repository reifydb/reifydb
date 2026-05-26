// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::FlowNodeId, key::operator_settings::OperatorSettingsKey, row::OperatorSettings,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use super::encode_operator_settings;
use crate::Result;

pub fn create_operator_settings(
	txn: &mut AdminTransaction,
	operator: FlowNodeId,
	settings: &OperatorSettings,
) -> Result<()> {
	let value = encode_operator_settings(settings);
	txn.set(&OperatorSettingsKey::encoded(operator), value)?;
	Ok(())
}
