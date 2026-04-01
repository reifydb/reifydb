// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionInspectorRef},
	value::column::columns::Columns,
};
use reifydb_type::{fragment::Fragment, params::Params, value::Value};

use crate::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};

pub struct InspectSubscription;

impl Default for InspectSubscription {
	fn default() -> Self {
		Self::new()
	}
}

impl InspectSubscription {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for InspectSubscription {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut reifydb_transaction::transaction::Transaction<'_>) -> Result<Columns, ProcedureError> {
		let subscription_id_value = match ctx.params {
			Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::Uint8(id) => *id,
				Value::Utf8(s) => s.parse::<u64>().map_err(|_| ProcedureError::ExecutionFailed {
					procedure: Fragment::internal("inspect_subscription"),
					reason: "Invalid subscription_id format".to_string(),
				})?,
				_ => {
					return Err(ProcedureError::ExecutionFailed {
						procedure: Fragment::internal("inspect_subscription"),
						reason: "subscription_id must be of type u64 or utf8".to_string(),
					});
				}
			},
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("inspect_subscription"),
					expected: 1,
					actual: match ctx.params {
						Params::Positional(args) => args.len(),
						_ => 0,
					},
				});
			}
		};

		let subscription_id = SubscriptionId(subscription_id_value);

		// Resolve SubscriptionInspector from IoC (registered by sub-subscription factory)
		let inspector =
			ctx.ioc.resolve::<SubscriptionInspectorRef>()
				.expect("SubscriptionInspector not registered in IoC");

		match inspector.inspect(subscription_id) {
			Some(columns) => Ok(columns),
			None => Ok(Columns::empty()),
		}
	}
}
