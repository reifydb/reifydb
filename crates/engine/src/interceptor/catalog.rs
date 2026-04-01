// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_transaction::interceptor::transaction::{PostCommitContext, PostCommitInterceptor};

use crate::Result;

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
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		let version = ctx.version;

		for change in &ctx.changes.namespace {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id())
				.expect("Change must have either pre or post state");
			self.catalog.set_namespace(id, version, change.post.clone());
		}

		for change in &ctx.changes.table {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|t| t.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_table(id, version, change.post.clone());
		}

		for change in &ctx.changes.view {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|v| v.id())
				.expect("Change must have either pre or post state");
			self.catalog.set_view(id, version, change.post.clone());
		}

		for change in &ctx.changes.ringbuffer {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|r| r.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_ringbuffer(id, version, change.post.clone());
		}

		for change in &ctx.changes.dictionary {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|d| d.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_dictionary(id, version, change.post.clone());
		}

		for change in &ctx.changes.procedure {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|p| p.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_procedure(id, version, change.post.clone());
		}

		for change in &ctx.changes.test {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|t| t.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_test(id, version, change.post.clone());
		}

		for change in &ctx.changes.handler {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|h| h.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_handler(id, version, change.post.clone());
		}

		for change in &ctx.changes.identity {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|u| u.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_identity(id, version, change.post.clone());
		}

		for change in &ctx.changes.role {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|r| r.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_role(id, version, change.post.clone());
		}

		for change in &ctx.changes.granted_role {
			let ur = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.expect("Change must have either pre or post state");
			self.catalog.set_granted_role(ur.identity, ur.role_id, version, change.post.clone());
		}

		for change in &ctx.changes.policy {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|p| p.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_policy(id, version, change.post.clone());
		}

		for change in &ctx.changes.migration {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|m| m.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_migration(id, version, change.post.clone());
		}

		for change in &ctx.changes.migration_event {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|e| e.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_migration_event(id, version, change.post.clone());
		}

		for change in &ctx.changes.sumtype {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_sumtype(id, version, change.post.clone());
		}

		for change in &ctx.changes.flow {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|f| f.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_flow(id, version, change.post.clone());
		}

		for change in &ctx.changes.source {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_source(id, version, change.post.clone());
		}

		for change in &ctx.changes.sink {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_sink(id, version, change.post.clone());
		}

		for (key, value) in &ctx.changes.config_changes {
			self.catalog.system_config().update(key, version, value.clone());
		}

		Ok(())
	}
}
