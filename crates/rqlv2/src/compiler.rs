// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Caching compiler for RQL scripts.

use std::sync::Arc;

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{ComputePool, util::LruCache};
use reifydb_hash::{Hash128, xxh3_128};

use crate::{CompiledProgram, RqlError, compile_script};

const DEFAULT_CACHE_CAPACITY: usize = 1000;

struct Inner {
	cache: LruCache<Hash128, CompiledProgram>,
	compute_pool: ComputePool,
	catalog: MaterializedCatalog,
}

/// A caching, async compiler for RQL scripts.
#[derive(Clone)]
pub struct Compiler {
	inner: Arc<Inner>,
}

impl Compiler {
	pub fn new(compute_pool: ComputePool, catalog: MaterializedCatalog) -> Self {
		Self {
			inner: Arc::new(Inner {
				cache: LruCache::new(DEFAULT_CACHE_CAPACITY),
				compute_pool,
				catalog,
			}),
		}
	}

	pub async fn compile(&self, source: &str) -> Result<CompiledProgram, RqlError> {
		let cache_key = xxh3_128(source.as_bytes());
		if let Some(program) = self.inner.cache.get(&cache_key) {
			return Ok(program);
		}

		// Cache miss: prepare for compilation
		let source_owned = source.to_string();
		let catalog = self.inner.catalog.clone();

		// Compile on ComputePool
		let program = self
			.inner
			.compute_pool
			.compute(move || compile_script(&source_owned, &catalog))
			.await
			.map_err(|join_err| {
			if join_err.is_panic() {
				RqlError::CompilationPanicked(format!("compilation task panicked: {:?}", join_err))
			} else {
				RqlError::CompilationPanicked("compilation task cancelled".to_string())
			}
		})??;

		// Insert into cache
		self.inner.cache.put(cache_key, program.clone());

		Ok(program)
	}

	pub fn clear_cache(&self) {
		self.inner.cache.clear();
	}

	pub fn cache_len(&self) -> usize {
		self.inner.cache.len()
	}
}
