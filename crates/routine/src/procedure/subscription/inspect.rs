// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionInspectorRef},
	value::column::columns::Columns,
};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("subscription::inspect"));

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

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for InspectSubscription {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let subscription_id_value = match ctx.params {
			Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::Uint8(id) => *id,
				Value::Utf8(s) => {
					s.parse::<u64>().map_err(|_| RoutineError::ProcedureExecutionFailed {
						procedure: Fragment::internal("subscription::inspect"),
						reason: "Invalid subscription_id format".to_string(),
					})?
				}
				_ => {
					return Err(RoutineError::ProcedureExecutionFailed {
						procedure: Fragment::internal("subscription::inspect"),
						reason: "subscription_id must be of type u64 or utf8".to_string(),
					});
				}
			},
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("subscription::inspect"),
					expected: 1,
					actual: match ctx.params {
						Params::Positional(args) => args.len(),
						_ => 0,
					},
				});
			}
		};

		let subscription_id = SubscriptionId(subscription_id_value);

		let inspector =
			ctx.ioc.resolve::<SubscriptionInspectorRef>()
				.expect("SubscriptionInspector not registered in IoC");

		match inspector.inspect(subscription_id) {
			Some(columns) => Ok(columns),
			None => Ok(Columns::empty()),
		}
	}
}
