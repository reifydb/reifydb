// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_transaction::interceptor::transaction::{PostCommitContext, PostCommitInterceptor};

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

impl PostCommitInterceptor for MaterializedCatalogInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()> {
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

		for change in &ctx.changes.ringbuffer_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|r| r.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_ringbuffer(id, version, change.post.clone());
		}

		for change in &ctx.changes.dictionary_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|d| d.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_dictionary(id, version, change.post.clone());
		}

		for change in &ctx.changes.procedure_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|p| p.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_procedure(id, version, change.post.clone());
		}

		for change in &ctx.changes.handler_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|h| h.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_handler(id, version, change.post.clone());
		}

		for change in &ctx.changes.user_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|u| u.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_user(id, version, change.post.clone());
		}

		for change in &ctx.changes.role_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|r| r.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_role(id, version, change.post.clone());
		}

		for change in &ctx.changes.user_role_def {
			let ur = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.expect("Change must have either pre or post state");
			self.catalog.set_user_role(ur.user_id, ur.role_id, version, change.post.clone());
		}

		for change in &ctx.changes.security_policy_def {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|p| p.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_security_policy(id, version, change.post.clone());
		}

		Ok(())
	}
}
