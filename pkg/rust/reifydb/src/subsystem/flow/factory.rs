// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, rc::Rc, time::Duration};

use reifydb_core::{
	interceptor::StandardInterceptorBuilder,
	interface::{ConsumerId, Transaction},
	ioc::IocContainer,
};
use reifydb_engine::StandardEngine;

use super::{FlowSubsystem, intercept::TransactionalFlowInterceptor};
use crate::subsystem::{Subsystem, factory::SubsystemFactory};

/// Factory for creating FlowSubsystem with proper interceptor registration
#[derive()]
pub struct FlowSubsystemFactory<T: Transaction> {
	consumer_id: ConsumerId,
	poll_interval: Duration,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> FlowSubsystemFactory<T> {
	pub fn new() -> Self {
		Self {
			consumer_id: ConsumerId::flow_consumer(),
			poll_interval: Duration::from_millis(1),
			_phantom: PhantomData,
		}
	}

	pub fn with_poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
	}
}

impl<T: Transaction> SubsystemFactory<T> for FlowSubsystemFactory<T> {
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T> {
		// Create factory functions that create new interceptor
		// instances
		let ioc_clone = ioc.clone();
		let ioc_clone2 = ioc.clone();
		let ioc_clone3 = ioc.clone();
		let ioc_clone4 = ioc.clone();

		builder.add_table_post_insert(move || {
			Rc::new(TransactionalFlowInterceptor::new(
				ioc_clone.clone(),
			))
		})
		.add_table_post_update(move || {
			Rc::new(TransactionalFlowInterceptor::new(
				ioc_clone2.clone(),
			))
		})
		.add_table_post_delete(move || {
			Rc::new(TransactionalFlowInterceptor::new(
				ioc_clone3.clone(),
			))
		})
		.add_pre_commit(move || {
			Rc::new(TransactionalFlowInterceptor::new(
				ioc_clone4.clone(),
			))
		})
	}

	fn create(
		self: Box<Self>,
		ioc: &IocContainer,
	) -> reifydb_core::Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine<T>>()?;

		Ok(Box::new(FlowSubsystem::new(
			engine,
			self.consumer_id,
			self.poll_interval,
		)))
	}
}
