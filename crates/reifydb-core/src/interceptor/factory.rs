// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{interceptor::Interceptors, interface::Transaction};

/// Factory trait for creating interceptor instances for each transaction
pub trait InterceptorFactory<T: Transaction>: Send + Sync {
	/// Create a new instance of interceptors for a transaction
	fn create(&self) -> Interceptors<T>;
}

/// Standard implementation of InterceptorFactory
pub struct StandardInterceptorFactory<T: Transaction> {
	pub(crate) interceptors: Interceptors<T>,
}

impl<T: Transaction> Default for StandardInterceptorFactory<T> {
	fn default() -> Self {
		Self {
			interceptors: Interceptors::new(),
		}
	}
}

impl<T: Transaction> InterceptorFactory<T> for StandardInterceptorFactory<T> {
	fn create(&self) -> Interceptors<T> {
		self.interceptors.clone()
	}
}
