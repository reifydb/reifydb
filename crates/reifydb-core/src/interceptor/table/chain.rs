// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Import the table interceptor traits and context types
use super::{context::*, *};
use crate::{interceptor::InterceptorChain, interface::Transaction};

// Implement chains for each interceptor type with specific contexts
impl<T: Transaction> InterceptorChain<T, dyn TablePreInsertInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: TablePreInsertContext<T>,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

impl<T: Transaction> InterceptorChain<T, dyn TablePostInsertInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: TablePostInsertContext<T>,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

impl<T: Transaction> InterceptorChain<T, dyn TablePreUpdateInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: TablePreUpdateContext<T>,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

impl<T: Transaction> InterceptorChain<T, dyn TablePostUpdateInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: TablePostUpdateContext<T>,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

impl<T: Transaction> InterceptorChain<T, dyn TablePreDeleteInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: TablePreDeleteContext<T>,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

impl<T: Transaction> InterceptorChain<T, dyn TablePostDeleteInterceptor<T>> {
	pub fn execute(
		&mut self,
		mut ctx: TablePostDeleteContext<T>,
	) -> crate::Result<()> {
		for interceptor in &mut self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}
