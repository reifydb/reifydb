// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use datafusion_common::DataFusionError;
use reifydb_core::internal_error;
use reifydb_value::error::Error;

pub(super) fn into_external(error: Error) -> DataFusionError {
	DataFusionError::External(Box::new(error))
}

pub(super) fn from_datafusion(operator: &str, err: DataFusionError) -> Error {
	if let DataFusionError::External(inner) = err.find_root()
		&& let Some(ours) = inner.downcast_ref::<Error>()
	{
		return Error(ours.0.clone());
	}
	internal_error!("{}: datafusion: {}", operator, err)
}
