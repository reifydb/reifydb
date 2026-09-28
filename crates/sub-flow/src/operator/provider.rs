// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use std::sync::Arc;

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_codec::value::encode_params;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_core::operator_with::encode_apply_with;
use reifydb_core::{interface::catalog::flow::OperatorId, operator_with::ApplyWith};
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_extension::operator::extern_c::loader::extern_c_operator_loader;
use reifydb_flow::error::FlowGraphError;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_flow_async::error::FlowStateError;
use reifydb_flow_async::operator::{BoxedHostOperator, provider::OperatorProvider};
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_value::params::Params;
use reifydb_value::{Result, config::ExtensionParams, error::Error};
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use tracing::instrument;

use crate::builder::CustomOperators;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use crate::{
	error::ExternOperatorError, operator::extern_c::ExternCOperatorHandle,
	operator::extern_rust::extern_rust_operator_loader,
};

pub struct StandardOperatorProvider {
	custom: CustomOperators,
}

impl StandardOperatorProvider {
	pub fn new(custom: CustomOperators) -> Self {
		Self {
			custom,
		}
	}
}

impl OperatorProvider for StandardOperatorProvider {
	fn provide(
		&self,
		operator_id: OperatorId,
		params: &ExtensionParams,
		with: &ApplyWith,
	) -> Result<BoxedHostOperator> {
		let operator = params.name();

		if let Some(factory) = self.custom.get(operator) {
			return factory(operator_id, params, with);
		}

		#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
		{
			if extern_rust_operator_loader().read().has_operator(operator) {
				return extern_rust_operator_loader().write().create_operator_by_name(
					operator,
					operator_id,
					params,
					with,
				);
			}

			if extern_c_operator_loader().read().has_operator(operator) {
				return self.create_extern_c_operator(operator, operator_id, params, with);
			}

			Err(Error::from(FlowGraphError::UnknownOperator {
				operator: operator.to_string(),
			}))
		}

		#[cfg(not(all(reifydb_target = "host", not(reifydb_dst))))]
		{
			Err(Error::from(FlowGraphError::ExternUnsupportedOnWasm))
		}
	}
}

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
impl StandardOperatorProvider {
	#[instrument(name = "flow::provider::create_extern_c_operator", level = "debug", skip(self, params, with), fields(operator = %operator, operator_id = ?operator_id))]
	fn create_extern_c_operator(
		&self,
		operator: &str,
		operator_id: OperatorId,
		params: &ExtensionParams,
		with: &ApplyWith,
	) -> Result<BoxedHostOperator> {
		let loader = extern_c_operator_loader();
		let mut loader_write = loader.write();

		let named = Params::Named(Arc::new(params.iter().map(|(k, v)| (k.clone(), v.clone())).collect()));
		let params_bytes = encode_params(&named).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "operator params",
				cause: e.to_string(),
			})
		})?;

		let with_bytes = encode_apply_with(with)?;

		let created = loader_write.create_operator_by_name(operator, operator_id, &params_bytes, &with_bytes);
		let (descriptor, class, instance) = match created {
			Ok(created) => created,
			Err(e) => {
				return Err(Error::from(ExternOperatorError::CreateFailed {
					cause: format!("{:?}", e),
				}));
			}
		};

		Ok(Box::new(ExternCOperatorHandle::new(descriptor, class, instance, operator_id)))
	}
}
