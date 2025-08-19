// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interceptor::StandardInterceptorBuilder, interface::Transaction,
};

use super::Subsystem;
use crate::ioc::IocContainer;

/// Factory trait for creating subsystems with IoC support
pub trait SubsystemFactory<T: Transaction> {
	/// Provide interceptors with access to IoC container
	fn provide_interceptors(
		&self,
		builder: StandardInterceptorBuilder<T>,
		ioc: &IocContainer,
	) -> StandardInterceptorBuilder<T>;

	/// Create the subsystem using services from IoC container
	fn create(self: Box<Self>, ioc: &IocContainer) -> Box<dyn Subsystem>;
}
