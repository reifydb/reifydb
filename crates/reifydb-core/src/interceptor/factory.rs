// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::Interceptors;
use crate::interface::Transaction;

/// Factory trait for creating interceptor instances for each transaction
pub trait InterceptorFactory<T: Transaction>: Send + Sync {
	/// Create a new instance of interceptors for a transaction
	fn create(&self) -> Interceptors<T>;
}
