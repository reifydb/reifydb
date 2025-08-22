// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, ops::Deref, sync::Arc};

use crate::{
	execute::Executor,
	function::{math, Functions},
};
use reifydb_core::interface::CommandTransaction;
use reifydb_core::{
	hook::{Hook, Hooks},
	interceptor::InterceptorFactory,
	interface::{
		Command, Engine as EngineInterface, ExecuteCommand,
		ExecuteQuery, GetHooks, Identity, Params, Query, Transaction,
		VersionedTransaction,
	},
	transaction::{StandardCommandTransaction, StandardQueryTransaction},
	Frame,
};

pub struct StandardEngine<T: Transaction>(Arc<EngineInner<T>>);

impl<T: Transaction> GetHooks for StandardEngine<T> {
	fn get_hooks(&self) -> &Hooks {
		&self.hooks
	}
}

impl<T: Transaction> EngineInterface<T> for StandardEngine<T> {
	fn begin_command(
		&self,
	) -> crate::Result<StandardCommandTransaction<T>> {
		let interceptors = self.interceptors.create();
		Ok(StandardCommandTransaction::new(
			self.versioned.begin_command()?,
			self.unversioned.clone(),
			self.cdc.clone(),
			self.hooks.clone(),
			interceptors,
		))
	}

	fn begin_query(&self) -> crate::Result<StandardQueryTransaction<T>> {
		Ok(StandardQueryTransaction::new(
			self.versioned.begin_query()?,
			self.unversioned.clone(),
			self.cdc.clone(),
		))
	}

	fn command_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: Params,
	) -> crate::Result<Vec<Frame>> {
		let mut txn = self.begin_command()?;
		let result = self.execute_command(
			&mut txn,
			Command {
				rql,
				params,
				identity,
			},
		)?;
		txn.commit()?;
		Ok(result)
	}

	fn query_as(
		&self,
		identity: &Identity,
		rql: &str,
		params: Params,
	) -> crate::Result<Vec<Frame>> {
		let mut txn = self.begin_query()?;
		let result = self.execute_query(
			&mut txn,
			Query {
				rql,
				params,
				identity,
			},
		)?;
		Ok(result)
	}
}

impl<T: Transaction> ExecuteCommand<T> for StandardEngine<T> {
	#[inline]
	fn execute_command<'a>(
		&'a self,
		txn: &mut impl CommandTransaction,
		cmd: Command<'a>,
	) -> crate::Result<Vec<Frame>> {
		self.executor.execute_command(txn, cmd)
	}
}

impl<T: Transaction> ExecuteQuery<T> for StandardEngine<T> {
	#[inline]
	fn execute_query<'a>(
		&'a self,
		txn: &mut StandardQueryTransaction<T>,
		qry: Query<'a>,
	) -> crate::Result<Vec<Frame>> {
		self.executor.execute_query(txn, qry)
	}
}

impl<T: Transaction> Clone for StandardEngine<T> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<T: Transaction> Deref for StandardEngine<T> {
	type Target = EngineInner<T>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub struct EngineInner<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	executor: Executor<T>,
	interceptors: Box<dyn InterceptorFactory<T>>,
}

impl<T: Transaction> StandardEngine<T> {
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
		interceptors: Box<dyn InterceptorFactory<T>>,
	) -> Self {
		Self(Arc::new(EngineInner {
			versioned: versioned.clone(),
			unversioned: unversioned.clone(),
			cdc: cdc.clone(),
			hooks,
			executor: Executor {
				functions: Functions::builder()
					.register_aggregate(
						"sum",
						math::aggregate::Sum::new,
					)
					.register_aggregate(
						"min",
						math::aggregate::Min::new,
					)
					.register_aggregate(
						"max",
						math::aggregate::Max::new,
					)
					.register_aggregate(
						"avg",
						math::aggregate::Avg::new,
					)
					.register_aggregate(
						"count",
						math::aggregate::Count::new,
					)
					.register_scalar(
						"abs",
						math::scalar::Abs::new,
					)
					.register_scalar(
						"avg",
						math::scalar::Avg::new,
					)
					.build(),
				_phantom: PhantomData,
			},
			interceptors,
		}))
	}

	#[inline]
	pub fn versioned(&self) -> &T::Versioned {
		&self.versioned
	}

	#[inline]
	pub fn versioned_owned(&self) -> T::Versioned {
		self.versioned.clone()
	}

	#[inline]
	pub fn unversioned(&self) -> &T::Unversioned {
		&self.unversioned
	}

	#[inline]
	pub fn unversioned_owned(&self) -> T::Unversioned {
		self.unversioned.clone()
	}

	#[inline]
	pub fn cdc(&self) -> &T::Cdc {
		&self.cdc
	}

	#[inline]
	pub fn cdc_owned(&self) -> T::Cdc {
		self.cdc.clone()
	}

	#[inline]
	pub fn trigger<H: Hook>(&self, hook: H) -> crate::Result<()> {
		self.hooks.trigger(hook)
	}
}
