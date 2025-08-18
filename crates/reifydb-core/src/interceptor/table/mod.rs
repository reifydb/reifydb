// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod chain;
pub mod context;

use self::context::{
	TablePostDeleteContext, TablePostInsertContext, TablePostUpdateContext,
	TablePreDeleteContext, TablePreInsertContext, TablePreUpdateContext,
};
use crate::interface::Transaction;

pub trait TablePreInsertInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut TablePreInsertContext<T>,
	) -> crate::Result<()>;
}

pub trait TablePostInsertInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&self,
		ctx: &mut TablePostInsertContext<T>,
	) -> crate::Result<()>;
}

pub trait TablePreUpdateInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&self,
		ctx: &mut TablePreUpdateContext<T>,
	) -> crate::Result<()>;
}

pub trait TablePostUpdateInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&self,
		ctx: &mut TablePostUpdateContext<T>,
	) -> crate::Result<()>;
}

pub trait TablePreDeleteInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&self,
		ctx: &mut TablePreDeleteContext<T>,
	) -> crate::Result<()>;
}

pub trait TablePostDeleteInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&self,
		ctx: &mut TablePostDeleteContext<T>,
	) -> crate::Result<()>;
}
