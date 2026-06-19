// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorSettingsChangeOperations, flow::FlowNodeId},
	key::operator_settings::OperatorSettingsKey,
	row::OperatorSettings,
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
	txn.track_operator_settings_created(operator, settings.clone())?;
	Ok(())
}
