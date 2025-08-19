// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	hook::Hooks,
	interceptor::{AddToBuilder, StandardInterceptorBuilder},
	interface::Transaction,
};

use super::DatabaseBuilder;
use crate::Database;

pub struct SyncBuilder<T: Transaction> {
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	hooks: Hooks,
	interceptors: StandardInterceptorBuilder<T>,
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
			interceptors: StandardInterceptorBuilder::new(),
		}
	}

	pub fn intercept<I>(mut self, interceptor: I) -> Self
	where
		I: AddToBuilder<T>,
	{
		self.interceptors =
			interceptor.add_to_builder(self.interceptors);
		self
	}

	pub fn build(self) -> Database<T> {
		DatabaseBuilder::new(
			self.versioned,
			self.unversioned,
			self.cdc,
			self.hooks,
		)
		.with_interceptor_builder(self.interceptors)
		.build()
	}
}
