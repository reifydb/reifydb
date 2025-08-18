// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod chain;
pub mod context;

use self::context::{PostCommitContext, PreCommitContext};
use crate::interface::Transaction;

pub trait PreCommitInterceptor<T: Transaction>: Send + Sync {
	fn intercept(&self, ctx: &mut PreCommitContext<T>)
	-> crate::Result<()>;
}

pub trait PostCommitInterceptor<T: Transaction>: Send + Sync {
	fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()>;
}
