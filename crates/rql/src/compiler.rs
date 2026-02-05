// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{fmt::Debug, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::util::lru::LruCache;
use reifydb_runtime::hash::{Hash128, xxh3_128};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::Result;

use crate::{
	ast::{ast::AstStatement, parse_str},
	plan::{physical::PhysicalPlan, plan},
};

const DEFAULT_CAPACITY: usize = 1024 * 8;

#[derive(Debug, Clone)]
pub struct CompiledPlan {
	pub plan: PhysicalPlan,
	pub is_output: bool,
}

/// Result of compiling a query.
pub enum CompilationResult {
	Ready(Arc<Vec<CompiledPlan>>),
	Incremental(IncrementalCompilation),
}

/// Opaque state for incremental compilation.
pub struct IncrementalCompilation {
	statements: Vec<AstStatement>,
	current: usize,
}

#[derive(Debug, Clone)]
pub struct Compiler(Arc<CompilerInner>);

struct CompilerInner {
	catalog: Catalog,
	cache: LruCache<Hash128, Arc<Vec<CompiledPlan>>>,
}

impl Debug for CompilerInner {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompilerInner")
			.field("catalog", &self.catalog)
			.field("cache_len", &self.cache.len())
			.field("cache_capacity", &self.cache.capacity())
			.finish()
	}
}

impl Compiler {
	pub fn new(catalog: Catalog) -> Self {
		Self(Arc::new(CompilerInner {
			catalog,
			cache: LruCache::new(DEFAULT_CAPACITY),
		}))
	}

	pub fn compile<T: AsTransaction>(&self, tx: &mut T, query: &str) -> Result<CompilationResult> {
		let hash = xxh3_128(query.as_bytes());

		if let Some(cached) = self.0.cache.get(&hash) {
			return Ok(CompilationResult::Ready(cached));
		}

		let statements = parse_str(query)?;
		let has_ddl = statements.iter().any(|s| s.contains_ddl());
		let needs_incremental = statements.len() > 1 && has_ddl;

		if needs_incremental {
			return Ok(CompilationResult::Incremental(IncrementalCompilation {
				statements,
				current: 0,
			}));
		}

		// Batch compile
		let mut plans = Vec::new();
		for statement in statements {
			let is_output = statement.is_output;
			if let Some(physical) = plan(&self.0.catalog, tx, statement)? {
				plans.push(CompiledPlan {
					plan: physical,
					is_output,
				});
			}
		}

		let arc_plans = Arc::new(plans);
		if !has_ddl {
			self.0.cache.put(hash, arc_plans.clone());
		}
		Ok(CompilationResult::Ready(arc_plans))
	}

	/// Compile the next statement in an incremental compilation.
	/// Returns `None` when all statements have been compiled.
	pub fn compile_next<T: AsTransaction>(
		&self,
		tx: &mut T,
		state: &mut IncrementalCompilation,
	) -> Result<Option<CompiledPlan>> {
		if state.current >= state.statements.len() {
			return Ok(None);
		}

		let statement = state.statements[state.current].clone();
		state.current += 1;

		let is_output = statement.is_output;
		if let Some(physical) = plan(&self.0.catalog, tx, statement)? {
			Ok(Some(CompiledPlan {
				plan: physical,
				is_output,
			}))
		} else {
			self.compile_next(tx, state)
		}
	}

	/// Clear all cached plans.
	pub fn clear(&self) {
		self.0.cache.clear();
	}

	/// Return the number of cached plans.
	pub fn len(&self) -> usize {
		self.0.cache.len()
	}

	/// Return true if the cache is empty.
	pub fn is_empty(&self) -> bool {
		self.0.cache.is_empty()
	}

	/// Return the cache capacity.
	pub fn capacity(&self) -> usize {
		self.0.cache.capacity()
	}
}
