// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{hook::Hooks, interface::Transaction};
use reifydb_engine::{
	StandardEngine, interceptor::InterceptorBuilder as InterceptorConfig,
};

use super::{DatabaseBuilder, InterceptorBuilder};
use crate::Database;

pub struct SyncBuilder<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	interceptor_config: InterceptorConfig<T>,
}

impl<T: Transaction> SyncBuilder<T> {
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
	) -> Self {
		Self {
			versioned,
			unversioned,
			cdc,
			hooks,
			interceptor_config: InterceptorConfig::new(),
		}
	}

	pub fn build(self) -> Database<T> {
		let engine = StandardEngine::new(
			self.versioned,
			self.unversioned,
			self.cdc,
			self.hooks,
			Box::new(self.interceptor_config.build()),
		);
		DatabaseBuilder::new(engine).build()
	}
}

impl<T: Transaction> InterceptorBuilder<T> for SyncBuilder<T> {
	fn builder(&mut self) -> &mut InterceptorConfig<T> {
		&mut self.interceptor_config
	}
}
