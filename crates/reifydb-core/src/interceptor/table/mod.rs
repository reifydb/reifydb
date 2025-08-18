// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod chain;
pub mod context;

use self::context::{
	TablePostDeleteContext, TablePostInsertContext, TablePostUpdateContext,
	TablePreDeleteContext, TablePreInsertContext, TablePreUpdateContext,
};
use crate::interface::Transaction;

/// Interceptor for table pre-insert operations
pub trait TablePreInsertInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut TablePreInsertContext<T>,
	) -> crate::Result<()>;
}

/// Interceptor for table post-insert operations
pub trait TablePostInsertInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut TablePostInsertContext<T>,
	) -> crate::Result<()>;
}

/// Interceptor for table pre-update operations
pub trait TablePreUpdateInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut TablePreUpdateContext<T>,
	) -> crate::Result<()>;
}

/// Interceptor for table post-update operations
pub trait TablePostUpdateInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut TablePostUpdateContext<T>,
	) -> crate::Result<()>;
}

/// Interceptor for table pre-delete operations
pub trait TablePreDeleteInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut TablePreDeleteContext<T>,
	) -> crate::Result<()>;
}

/// Interceptor for table post-delete operations
pub trait TablePostDeleteInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut TablePostDeleteContext<T>,
	) -> crate::Result<()>;
}
