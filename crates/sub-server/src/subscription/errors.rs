// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

use crate::execute::ExecuteError;

pub enum CreateSubscriptionError {
	Execute(ExecuteError),
	ExtractionFailed,
}

impl fmt::Display for CreateSubscriptionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CreateSubscriptionError::Execute(e) => write!(f, "{}", e),
			CreateSubscriptionError::ExtractionFailed => write!(f, "Failed to extract subscription ID"),
		}
	}
}

impl fmt::Debug for CreateSubscriptionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CreateSubscriptionError::Execute(e) => f.debug_tuple("Execute").field(e).finish(),
			CreateSubscriptionError::ExtractionFailed => write!(f, "ExtractionFailed"),
		}
	}
}

impl From<ExecuteError> for CreateSubscriptionError {
	fn from(err: ExecuteError) -> Self {
		CreateSubscriptionError::Execute(err)
	}
}
