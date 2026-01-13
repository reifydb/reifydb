// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Caching compiler for RQL scripts.

use std::sync::Arc;

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::util::ConcurrentLruCache;
use reifydb_hash::{Hash128, xxh3_128};

use crate::{CompiledProgram, RqlError, compile_script};

const DEFAULT_CACHE_CAPACITY: usize = 1000;

struct Inner {
	cache: ConcurrentLruCache<Hash128, CompiledProgram>,
	catalog: MaterializedCatalog,
}

/// A caching compiler for RQL scripts.
#[derive(Clone)]
pub struct Compiler {
	inner: Arc<Inner>,
}

impl Compiler {
	pub fn new(catalog: MaterializedCatalog) -> Self {
		Self {
			inner: Arc::new(Inner {
				cache: ConcurrentLruCache::new(DEFAULT_CACHE_CAPACITY),
				catalog,
			}),
		}
	}

	pub fn compile(&self, source: &str) -> Result<CompiledProgram, RqlError> {
		let cache_key = xxh3_128(source.as_bytes());
		if let Some(program) = self.inner.cache.get(&cache_key) {
			return Ok(program.clone());
		}

		// Cache miss: compile directly
		let program = compile_script(source, &self.inner.catalog)?;

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
