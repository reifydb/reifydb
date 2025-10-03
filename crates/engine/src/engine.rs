// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, rc::Rc, sync::Arc};

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	Frame,
	event::{Event, EventBus},
	interceptor::InterceptorFactory,
	interface::{
		Command, Engine as EngineInterface, ExecuteCommand, ExecuteQuery, Identity, MultiVersionTransaction,
		Params, Query, WithEventBus,
	},
};
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion};

use crate::{
	execute::Executor,
	function::{Functions, math},
	interceptor::materialized_catalog::MaterializedCatalogInterceptor,
	transaction::{StandardCommandTransaction, StandardQueryTransaction},
};

pub struct StandardEngine(Arc<EngineInner>);

impl WithEventBus for StandardEngine {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl EngineInterface for StandardEngine {
	type Command = StandardCommandTransaction;
	type Query = StandardQueryTransaction;

	fn begin_command(&self) -> crate::Result<Self::Command> {
		let mut interceptors = self.interceptors.create();

		interceptors.post_commit.add(Rc::new(MaterializedCatalogInterceptor::new(self.catalog.clone())));

		StandardCommandTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.cdc.clone(),
			self.event_bus.clone(),
			self.catalog.clone(),
			interceptors,
		)
	}

	fn begin_query(&self) -> crate::Result<Self::Query> {
		Ok(StandardQueryTransaction::new(
			self.multi.begin_query()?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		))
	}

	fn command_as(&self, identity: &Identity, rql: &str, params: Params) -> crate::Result<Vec<Frame>> {
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

	fn query_as(&self, identity: &Identity, rql: &str, params: Params) -> crate::Result<Vec<Frame>> {
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

impl ExecuteCommand<StandardCommandTransaction> for StandardEngine {
	#[inline]
	fn execute_command(&self, txn: &mut StandardCommandTransaction, cmd: Command<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_command(txn, cmd)
	}
}

impl ExecuteQuery<StandardQueryTransaction> for StandardEngine {
	#[inline]
	fn execute_query(&self, txn: &mut StandardQueryTransaction, qry: Query<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_query(txn, qry)
	}
}

impl Clone for StandardEngine {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Deref for StandardEngine {
	type Target = EngineInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub struct EngineInner {
	multi: TransactionMultiVersion,
	single: TransactionSingleVersion,
	cdc: TransactionCdc,
	event_bus: EventBus,
	executor: Executor,
	interceptors: Box<dyn InterceptorFactory<StandardCommandTransaction>>,
	catalog: MaterializedCatalog,
}

impl StandardEngine {
	pub fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingleVersion,
		cdc: TransactionCdc,
		event_bus: EventBus,
		interceptors: Box<dyn InterceptorFactory<StandardCommandTransaction>>,
		catalog: MaterializedCatalog,
	) -> Self {
		Self(Arc::new(EngineInner {
			multi,
			single,
			cdc,
			event_bus,
			executor: Executor::new(
				Functions::builder()
					.register_aggregate("sum", math::aggregate::Sum::new)
					.register_aggregate("min", math::aggregate::Min::new)
					.register_aggregate("max", math::aggregate::Max::new)
					.register_aggregate("avg", math::aggregate::Avg::new)
					.register_aggregate("count", math::aggregate::Count::new)
					.register_scalar("abs", math::scalar::Abs::new)
					.register_scalar("avg", math::scalar::Avg::new)
					.build(),
			),
			interceptors,
			catalog,
		}))
	}

	#[inline]
	pub fn multi(&self) -> &TransactionMultiVersion {
		&self.multi
	}

	#[inline]
	pub fn multi_owned(&self) -> TransactionMultiVersion {
		self.multi.clone()
	}

	#[inline]
	pub fn single(&self) -> &TransactionSingleVersion {
		&self.single
	}

	#[inline]
	pub fn single_owned(&self) -> TransactionSingleVersion {
		self.single.clone()
	}

	#[inline]
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}

	#[inline]
	pub fn cdc_owned(&self) -> TransactionCdc {
		self.cdc.clone()
	}

	#[inline]
	pub fn emit<E: Event>(&self, event: E) {
		self.event_bus.emit(event)
	}

	#[inline]
	pub fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}

	#[inline]
	pub fn executor(&self) -> Executor {
		self.executor.clone()
	}
}
