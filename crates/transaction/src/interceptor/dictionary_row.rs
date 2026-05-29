// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::dictionary::Dictionary;
use reifydb_value::{
	Result,
	value::{Value, dictionary::DictionaryEntryId},
};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct DictionaryRowPreInsertContext<'a> {
	pub dictionary: &'a Dictionary,
	pub values: &'a mut [Value],
}

impl<'a> DictionaryRowPreInsertContext<'a> {
	pub fn new(dictionary: &'a Dictionary, values: &'a mut [Value]) -> Self {
		Self {
			dictionary,
			values,
		}
	}
}

pub trait DictionaryRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPreInsertContext) -> Result<()> {
		let original_len = ctx.values.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.values.len(), original_len, "pre_insert interceptor changed values count");
		}
		Ok(())
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
	pub ids: &'a [DictionaryEntryId],
	pub values: &'a [Value],
}

impl<'a> DictionaryRowPostInsertContext<'a> {
	pub fn new(dictionary: &'a Dictionary, ids: &'a [DictionaryEntryId], values: &'a [Value]) -> Self {
		assert_eq!(ids.len(), values.len(), "ids/values length mismatch");
		Self {
			dictionary,
			ids,
			values,
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
	pub ids: &'a [DictionaryEntryId],
	pub values: &'a mut [Value],
}

impl<'a> DictionaryRowPreUpdateContext<'a> {
	pub fn new(dictionary: &'a Dictionary, ids: &'a [DictionaryEntryId], values: &'a mut [Value]) -> Self {
		assert_eq!(ids.len(), values.len(), "ids/values length mismatch");
		Self {
			dictionary,
			ids,
			values,
		}
	}
}

pub trait DictionaryRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryRowPreUpdateContext) -> Result<()> {
		let original_len = ctx.values.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.values.len(), original_len, "pre_update interceptor changed values count");
		}
		Ok(())
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
	pub ids: &'a [DictionaryEntryId],
	pub posts: &'a [Value],
	pub pres: &'a [Value],
}

impl<'a> DictionaryRowPostUpdateContext<'a> {
	pub fn new(
		dictionary: &'a Dictionary,
		ids: &'a [DictionaryEntryId],
		posts: &'a [Value],
		pres: &'a [Value],
	) -> Self {
		assert_eq!(ids.len(), posts.len(), "ids/posts length mismatch");
		assert_eq!(ids.len(), pres.len(), "ids/pres length mismatch");
		Self {
			dictionary,
			ids,
			posts,
			pres,
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
	pub ids: &'a [DictionaryEntryId],
}

impl<'a> DictionaryRowPreDeleteContext<'a> {
	pub fn new(dictionary: &'a Dictionary, ids: &'a [DictionaryEntryId]) -> Self {
		Self {
			dictionary,
			ids,
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
	pub ids: &'a [DictionaryEntryId],
	pub values: &'a [Value],
}

impl<'a> DictionaryRowPostDeleteContext<'a> {
	pub fn new(dictionary: &'a Dictionary, ids: &'a [DictionaryEntryId], values: &'a [Value]) -> Self {
		assert_eq!(ids.len(), values.len(), "ids/values length mismatch");
		Self {
			dictionary,
			ids,
			values,
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
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		values: &mut [Value],
	) -> Result<()> {
		let ctx = DictionaryRowPreInsertContext::new(dictionary, values);
		txn.dictionary_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
		values: &[Value],
	) -> Result<()> {
		let ctx = DictionaryRowPostInsertContext::new(dictionary, ids, values);
		txn.dictionary_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
		values: &mut [Value],
	) -> Result<()> {
		let ctx = DictionaryRowPreUpdateContext::new(dictionary, ids, values);
		txn.dictionary_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
		posts: &[Value],
		pres: &[Value],
	) -> Result<()> {
		let ctx = DictionaryRowPostUpdateContext::new(dictionary, ids, posts, pres);
		txn.dictionary_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
	) -> Result<()> {
		let ctx = DictionaryRowPreDeleteContext::new(dictionary, ids);
		txn.dictionary_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		dictionary: &Dictionary,
		ids: &[DictionaryEntryId],
		values: &[Value],
	) -> Result<()> {
		let ctx = DictionaryRowPostDeleteContext::new(dictionary, ids, values);
		txn.dictionary_row_post_delete_interceptors().execute(ctx)
	}
}
