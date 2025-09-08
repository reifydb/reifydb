// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	interceptor::{PostCommitContext, PostCommitInterceptor},
	interface::Transaction,
};

use crate::StandardCommandTransaction;

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

		for change in &ctx.changes.schema_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id)
				.expect(
					"Change must have either pre or post state",
				);
			self.catalog.set_schema(
				id,
				version,
				change.post.clone(),
			);
		}

		for change in &ctx.changes.table_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|t| t.id)
				.expect(
					"Change must have either pre or post state",
				);
			self.catalog.set_table(
				id,
				version,
				change.post.clone(),
			);
		}

		for change in &ctx.changes.view_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|v| v.id)
				.expect(
					"Change must have either pre or post state",
				);
			self.catalog.set_view(id, version, change.post.clone());
		}

		Ok(())
	}
}
