// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::dictionary::DictionaryDef;
use reifydb_type::{
	Result,
	value::{Value, dictionary::DictionaryEntryId},
};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

// PRE INSERT
/// Context for dictionary pre-insert interceptors
pub struct DictionaryPreInsertContext<'a> {
	pub dictionary: &'a DictionaryDef,
	pub value: Value,
}

impl<'a> DictionaryPreInsertContext<'a> {
	pub fn new(dictionary: &'a DictionaryDef, value: Value) -> Self {
		Self {
			dictionary,
			value,
		}
	}
}

pub trait DictionaryPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPreInsertContext) -> Result<Value> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.value)
	}
}

pub struct ClosureDictionaryPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPreInsertInterceptor for ClosureDictionaryPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_pre_insert<F>(f: F) -> ClosureDictionaryPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPreInsertInterceptor::new(f)
}

// POST INSERT
/// Context for dictionary post-insert interceptors
pub struct DictionaryPostInsertContext<'a> {
	pub dictionary: &'a DictionaryDef,
	pub id: DictionaryEntryId,
	pub value: &'a Value,
}

impl<'a> DictionaryPostInsertContext<'a> {
	pub fn new(dictionary: &'a DictionaryDef, id: DictionaryEntryId, value: &'a Value) -> Self {
		Self {
			dictionary,
			id,
			value,
		}
	}
}

pub trait DictionaryPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPostInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPostInsertInterceptor for ClosureDictionaryPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_post_insert<F>(f: F) -> ClosureDictionaryPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPostInsertInterceptor::new(f)
}

// PRE UPDATE
/// Context for dictionary pre-update interceptors
pub struct DictionaryPreUpdateContext<'a> {
	pub dictionary: &'a DictionaryDef,
	pub id: DictionaryEntryId,
	pub value: Value,
}

impl<'a> DictionaryPreUpdateContext<'a> {
	pub fn new(dictionary: &'a DictionaryDef, id: DictionaryEntryId, value: Value) -> Self {
		Self {
			dictionary,
			id,
			value,
		}
	}
}

pub trait DictionaryPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPreUpdateContext) -> Result<Value> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.value)
	}
}

pub struct ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPreUpdateInterceptor for ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_pre_update<F>(f: F) -> ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPreUpdateInterceptor::new(f)
}

// POST UPDATE
/// Context for dictionary post-update interceptors
pub struct DictionaryPostUpdateContext<'a> {
	pub dictionary: &'a DictionaryDef,
	pub id: DictionaryEntryId,
	pub value: &'a Value,
	pub old_value: &'a Value,
}

impl<'a> DictionaryPostUpdateContext<'a> {
	pub fn new(
		dictionary: &'a DictionaryDef,
		id: DictionaryEntryId,
		value: &'a Value,
		old_value: &'a Value,
	) -> Self {
		Self {
			dictionary,
			id,
			value,
			old_value,
		}
	}
}

pub trait DictionaryPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPostUpdateInterceptor for ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_post_update<F>(f: F) -> ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPostUpdateInterceptor::new(f)
}

// PRE DELETE
/// Context for dictionary pre-delete interceptors
pub struct DictionaryPreDeleteContext<'a> {
	pub dictionary: &'a DictionaryDef,
	pub id: DictionaryEntryId,
}

impl<'a> DictionaryPreDeleteContext<'a> {
	pub fn new(dictionary: &'a DictionaryDef, id: DictionaryEntryId) -> Self {
		Self {
			dictionary,
			id,
		}
	}
}

pub trait DictionaryPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPreDeleteInterceptor for ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_pre_delete<F>(f: F) -> ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPreDeleteInterceptor::new(f)
}

// POST DELETE
/// Context for dictionary post-delete interceptors
pub struct DictionaryPostDeleteContext<'a> {
	pub dictionary: &'a DictionaryDef,
	pub id: DictionaryEntryId,
	pub value: &'a Value,
}

impl<'a> DictionaryPostDeleteContext<'a> {
	pub fn new(dictionary: &'a DictionaryDef, id: DictionaryEntryId, value: &'a Value) -> Self {
		Self {
			dictionary,
			id,
			value,
		}
	}
}

pub trait DictionaryPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPostDeleteInterceptor for ClosureDictionaryPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_post_delete<F>(f: F) -> ClosureDictionaryPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPostDeleteInterceptor::new(f)
}

/// Helper struct for executing dictionary interceptors via static methods.
pub struct DictionaryInterceptor;

impl DictionaryInterceptor {
	pub fn pre_insert(txn: &mut impl WithInterceptors, dictionary: &DictionaryDef, value: Value) -> Result<Value> {
		let ctx = DictionaryPreInsertContext::new(dictionary, value);
		txn.dictionary_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
		value: &Value,
	) -> Result<()> {
		let ctx = DictionaryPostInsertContext::new(dictionary, id, value);
		txn.dictionary_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
		value: Value,
	) -> Result<Value> {
		let ctx = DictionaryPreUpdateContext::new(dictionary, id, value);
		txn.dictionary_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
		value: &Value,
		old_value: &Value,
	) -> Result<()> {
		let ctx = DictionaryPostUpdateContext::new(dictionary, id, value, old_value);
		txn.dictionary_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(
		txn: &mut impl WithInterceptors,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
	) -> Result<()> {
		let ctx = DictionaryPreDeleteContext::new(dictionary, id);
		txn.dictionary_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
		value: &Value,
	) -> Result<()> {
		let ctx = DictionaryPostDeleteContext::new(dictionary, id, value);
		txn.dictionary_post_delete_interceptors().execute(ctx)
	}
}
