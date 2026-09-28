// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, operator_with::ApplyWith};
use reifydb_flow::error::FlowGraphError;
use reifydb_value::{Result, config::ExtensionParams, error::Error};

use crate::operator::BoxedHostOperator;

pub trait OperatorProvider: Send + Sync {
	fn provide(
		&self,
		operator_id: OperatorId,
		params: &ExtensionParams,
		with: &ApplyWith,
	) -> Result<BoxedHostOperator>;
}

pub struct EmptyOperatorProvider;

impl OperatorProvider for EmptyOperatorProvider {
	fn provide(
		&self,
		_operator_id: OperatorId,
		params: &ExtensionParams,
		_with: &ApplyWith,
	) -> Result<BoxedHostOperator> {
		Err(Error::from(FlowGraphError::UnknownOperator {
			operator: params.name().to_string(),
		}))
	}
}
