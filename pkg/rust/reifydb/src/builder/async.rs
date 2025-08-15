// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{hook::Hooks, interface::Transaction};
use reifydb_engine::StandardEngine;

use super::DatabaseBuilder;
use crate::{Database, hook::WithHooks};

#[cfg(feature = "async")]
pub struct AsyncBuilder<T: Transaction> {
	inner: DatabaseBuilder<T>,
	engine: StandardEngine<T>,
}

#[cfg(feature = "async")]
impl<T: Transaction> AsyncBuilder<T> {
	pub fn new(
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
	) -> Self {
		let engine = StandardEngine::new(
			versioned,
			unversioned,
			cdc,
			hooks.clone(),
		)
		.unwrap();
		let inner = DatabaseBuilder::new(engine.clone());
		Self {
			inner,
			engine,
		}
	}

	pub fn build(self) -> Database<T> {
		self.inner.build()
	}
}

#[cfg(feature = "async")]
impl<T: Transaction> WithHooks<T> for AsyncBuilder<T> {
	fn engine(&self) -> &StandardEngine<T> {
		&self.engine
	}
}
