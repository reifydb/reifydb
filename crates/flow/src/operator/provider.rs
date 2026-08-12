// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::{Result, config::Config, error::Error};

use crate::{error::FlowGraphError, operator::BoxedOperator};

pub trait OperatorProvider: Send + Sync {
	fn provide(&self, operator_id: OperatorId, config: &Config) -> Result<BoxedOperator>;
}

pub struct EmptyOperatorProvider;

impl OperatorProvider for EmptyOperatorProvider {
	fn provide(&self, _operator_id: OperatorId, config: &Config) -> Result<BoxedOperator> {
		Err(Error::from(FlowGraphError::UnknownOperator {
			operator: config.name().to_string(),
		}))
	}
}
