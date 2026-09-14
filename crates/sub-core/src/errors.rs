// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

pub enum CreateSubscriptionError<E> {
	Execute(E),
}

impl<E: fmt::Display> fmt::Display for CreateSubscriptionError<E> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CreateSubscriptionError::Execute(e) => write!(f, "{}", e),
		}
	}
}

impl<E: fmt::Debug> fmt::Debug for CreateSubscriptionError<E> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CreateSubscriptionError::Execute(e) => f.debug_tuple("Execute").field(e).finish(),
		}
	}
}
