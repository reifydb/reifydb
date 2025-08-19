// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{interceptor::StandardInterceptorBuilder, interface::Transaction};

/// Trait for providing interceptors without requiring an engine instance
pub trait InterceptorProvider<T: Transaction> {
	/// Add interceptors to the builder
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
	) -> StandardInterceptorBuilder<T>;
}
