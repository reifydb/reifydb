// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::dictionary::Dictionary;
use reifydb_type::{
	Result,
	value::{Value, dictionary::DictionaryEntryId},
};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct DictionaryRowPreInsertContext<'a> {
	pub dictionary: &'a Dictionary,
	pub value: Value,
}

impl<'a> DictionaryRowPreInsertContext<'a> {
	pub fn new(dictionary: &'a Dictionary, value: Value) -> Self {
		Self {
			dictionary,
			value,
		}
	}
}

pub trait DictionaryRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPreInsertContext) -> Result<Value> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.value)
	}
}

pub struct ClosureDictionaryRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryRowPreInsertInterceptor for ClosureDictionaryRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_row_pre_insert<F>(f: F) -> ClosureDictionaryRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryRowPreInsertInterceptor::new(f)
}

pub struct DictionaryRowPostInsertContext<'a> {
	pub dictionary: &'a Dictionary,
	pub id: DictionaryEntryId,
	pub value: &'a Value,
}

impl<'a> DictionaryRowPostInsertContext<'a> {
	pub fn new(dictionary: &'a Dictionary, id: DictionaryEntryId, value: &'a Value) -> Self {
		Self {
			dictionary,
			id,
			value,
		}
	}
}

pub trait DictionaryRowPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPostInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryRowPostInsertInterceptor for ClosureDictionaryRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_row_post_insert<F>(f: F) -> ClosureDictionaryRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryRowPostInsertInterceptor::new(f)
}

pub struct DictionaryRowPreUpdateContext<'a> {
	pub dictionary: &'a Dictionary,
	pub id: DictionaryEntryId,
	pub value: Value,
}

impl<'a> DictionaryRowPreUpdateContext<'a> {
	pub fn new(dictionary: &'a Dictionary, id: DictionaryEntryId, value: Value) -> Self {
		Self {
			dictionary,
			id,
			value,
		}
	}
}

pub trait DictionaryRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPreUpdateContext) -> Result<Value> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.value)
	}
}

pub struct ClosureDictionaryRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryRowPreUpdateInterceptor for ClosureDictionaryRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_row_pre_update<F>(f: F) -> ClosureDictionaryRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryRowPreUpdateInterceptor::new(f)
}

pub struct DictionaryRowPostUpdateContext<'a> {
	pub dictionary: &'a Dictionary,
	pub id: DictionaryEntryId,
	pub post: &'a Value,
	pub pre: &'a Value,
}

impl<'a> DictionaryRowPostUpdateContext<'a> {
	pub fn new(dictionary: &'a Dictionary, id: DictionaryEntryId, post: &'a Value, pre: &'a Value) -> Self {
		Self {
			dictionary,
			id,
			post,
			pre,
		}
	}
}

pub trait DictionaryRowPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryRowPostUpdateInterceptor for ClosureDictionaryRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_row_post_update<F>(f: F) -> ClosureDictionaryRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryRowPostUpdateInterceptor::new(f)
}

pub struct DictionaryRowPreDeleteContext<'a> {
	pub dictionary: &'a Dictionary,
	pub id: DictionaryEntryId,
}

impl<'a> DictionaryRowPreDeleteContext<'a> {
	pub fn new(dictionary: &'a Dictionary, id: DictionaryEntryId) -> Self {
		Self {
			dictionary,
			id,
		}
	}
}

pub trait DictionaryRowPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryRowPreDeleteInterceptor for ClosureDictionaryRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_row_pre_delete<F>(f: F) -> ClosureDictionaryRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryRowPreDeleteInterceptor::new(f)
}

pub struct DictionaryRowPostDeleteContext<'a> {
	pub dictionary: &'a Dictionary,
	pub id: DictionaryEntryId,
	pub value: &'a Value,
}

impl<'a> DictionaryRowPostDeleteContext<'a> {
	pub fn new(dictionary: &'a Dictionary, id: DictionaryEntryId, value: &'a Value) -> Self {
		Self {
			dictionary,
			id,
			value,
		}
	}
}

pub trait DictionaryRowPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryRowPostDeleteInterceptor for ClosureDictionaryRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_row_post_delete<F>(f: F) -> ClosureDictionaryRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryRowPostDeleteInterceptor::new(f)
}

pub struct DictionaryRowInterceptor;

impl DictionaryRowInterceptor {
	pub fn pre_insert(txn: &mut impl WithInterceptors, dictionary: &Dictionary, value: Value) -> Result<Value> {
		let ctx = DictionaryRowPreInsertContext::new(dictionary, value);
		txn.dictionary_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		id: DictionaryEntryId,
		value: &Value,
	) -> Result<()> {
		let ctx = DictionaryRowPostInsertContext::new(dictionary, id, value);
		txn.dictionary_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		id: DictionaryEntryId,
		value: Value,
	) -> Result<Value> {
		let ctx = DictionaryRowPreUpdateContext::new(dictionary, id, value);
		txn.dictionary_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		id: DictionaryEntryId,
		post: &Value,
		pre: &Value,
	) -> Result<()> {
		let ctx = DictionaryRowPostUpdateContext::new(dictionary, id, post, pre);
		txn.dictionary_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		id: DictionaryEntryId,
	) -> Result<()> {
		let ctx = DictionaryRowPreDeleteContext::new(dictionary, id);
		txn.dictionary_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		id: DictionaryEntryId,
		value: &Value,
	) -> Result<()> {
		let ctx = DictionaryRowPostDeleteContext::new(dictionary, id, value);
		txn.dictionary_row_post_delete_interceptors().execute(ctx)
	}
}
