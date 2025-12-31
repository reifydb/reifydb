// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_catalog::MaterializedCatalog;
use reifydb_core::interceptor::{PostCommitContext, PostCommitInterceptor};
use reifydb_transaction::StandardCommandTransaction;

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

#[async_trait]
impl PostCommitInterceptor<StandardCommandTransaction> for MaterializedCatalogInterceptor {
	async fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()> {
		let version = ctx.version;

		for change in &ctx.changes.namespace_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_namespace(id, version, change.post.clone());
		}

		for change in &ctx.changes.table_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|t| t.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_table(id, version, change.post.clone());
		}

		for change in &ctx.changes.view_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|v| v.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_view(id, version, change.post.clone());
		}

		Ok(())
	}
}
