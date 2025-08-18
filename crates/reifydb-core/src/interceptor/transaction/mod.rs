// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod chain;
pub mod context;

use self::context::{PostCommitContext, PreCommitContext};
use crate::interface::Transaction;

/// Interceptor for pre-commit operations
pub trait PreCommitInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut PreCommitContext<T>,
	) -> crate::Result<()>;
}

/// Interceptor for post-commit operations
pub trait PostCommitInterceptor<T: Transaction>: Send + Sync {
	fn intercept(
		&mut self,
		ctx: &mut PostCommitContext,
	) -> crate::Result<()>;
}
