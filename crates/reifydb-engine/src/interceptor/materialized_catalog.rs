// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::StandardCommandTransaction;
use reifydb_catalog::MaterializedCatalog;
use reifydb_core::interceptor::{PostCommitContext, PostCommitInterceptor};
use reifydb_core::interface::Transaction;

pub(crate) struct MaterializedCatalogInterceptor {
	catalog: MaterializedCatalog,
}

impl MaterializedCatalogInterceptor {
	pub fn new(catalog: MaterializedCatalog) -> Self {
		Self {
			catalog,
		}
	}
}

impl<T: Transaction> PostCommitInterceptor<StandardCommandTransaction<T>>
	for MaterializedCatalogInterceptor
{
	fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()> {
		let version = ctx.version;

		for (id, change) in &ctx.changes.schema_def {
			self.catalog.set_schema(
				*id,
				version,
				change.post.clone(),
			);
		}

		for (id, change) in &ctx.changes.table_def {
			self.catalog.set_table(
				*id,
				version,
				change.post.clone(),
			);
		}

		for (id, change) in &ctx.changes.view_def {
			self.catalog.set_view(
				*id,
				version,
				change.post.clone(),
			);
		}

		Ok(())
	}
}
