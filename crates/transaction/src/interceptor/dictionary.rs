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
}
