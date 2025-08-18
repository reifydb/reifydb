// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{context::*, *};
use crate::{interceptor::InterceptorChain, interface::Transaction};

impl<T: Transaction> InterceptorChain<T, dyn PreCommitInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: PreCommitContext<T>,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

impl<T: Transaction> InterceptorChain<T, dyn PostCommitInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: PostCommitContext,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}
