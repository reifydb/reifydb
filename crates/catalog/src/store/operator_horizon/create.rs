// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorHorizonChangeOperations, flow::OperatorId},
	key::operator_horizon::OperatorHorizonKey,
	row::OperatorHorizon,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::error::ValueError;

use super::encode_operator_horizon;

pub fn create_operator_horizon(
	txn: &mut AdminTransaction,
	operator: OperatorId,
	horizon: &OperatorHorizon,
) -> Result<(), ValueError> {
	let value = encode_operator_horizon(horizon);
	txn.set(vec![(OperatorHorizonKey::new(operator).into(), value.into())])?;
	txn.track_operator_horizon_created(operator, horizon.clone())?;
	Ok(())
}
