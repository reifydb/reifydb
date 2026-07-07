// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_transaction::interceptor::transaction::{PostCommitContext, PostCommitInterceptor};

use crate::{Result, cache::CatalogCache, catalog::Catalog};

pub struct CatalogCacheInterceptor {
	catalog: CatalogCache,
}

impl CatalogCacheInterceptor {
	pub fn new(catalog: &Catalog) -> Self {
		Self {
			catalog: catalog.cache.clone(),
		}
	}
}

impl PostCommitInterceptor for CatalogCacheInterceptor {
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

		for change in &ctx.changes.relationship {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|r| r.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_relationship(id, version, change.post.clone());
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

		for change in &ctx.changes.series {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_series(id, version, change.post.clone());
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

		for change in &ctx.changes.column_snapshot {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|s| s.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_column_snapshot(id, version, change.post.clone());
		}

		for change in &ctx.changes.procedure {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|p| p.id())
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

		for change in &ctx.changes.identity_attribute {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|a| a.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_identity_attribute(id, version, change.post.clone());
		}

		for change in &ctx.changes.identity_attribute_value {
			let value = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.expect("Change must have either pre or post state");
			self.catalog.set_identity_attribute_value(
				value.identity,
				value.attribute,
				version,
				change.post.clone(),
			);
		}

		for change in &ctx.changes.authentication {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|a| a.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_authentication(id, version, change.post.clone());
		}

		for change in &ctx.changes.binding {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|b| b.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_binding(id, version, change.post.clone());
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

		for change in &ctx.changes.flow_node {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|n| n.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_flow_node(id, version, change.post.clone());
		}

		for change in &ctx.changes.flow_edge {
			let id = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|e| e.id)
				.expect("Change must have either pre or post state");
			self.catalog.set_flow_edge(id, version, change.post.clone());
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

		for change in &ctx.changes.config {
			let key = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.map(|c| c.key)
				.expect("Change must have either pre or post state");
			if let Some(post) = &change.post {
				self.catalog.set_config(key, version, post.value.clone())?;
			}
		}

		for change in &ctx.changes.row_settings {
			let (shape, _) = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.expect("Change must have either pre or post state");
			let settings = change.post.as_ref().map(|(_, settings)| settings.clone());
			self.catalog.set_row_settings(*shape, version, settings);
		}

		for change in &ctx.changes.operator_settings {
			let (operator, _) = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.expect("Change must have either pre or post state");
			let settings = change.post.as_ref().map(|(_, settings)| settings.clone());
			self.catalog.set_operator_settings(*operator, version, settings);
		}

		for change in &ctx.changes.primary_key {
			let (shape, primary_key) = change
				.post
				.as_ref()
				.or(change.pre.as_ref())
				.expect("Change must have either pre or post state");
			let post = change.post.as_ref().map(|(_, pk)| pk.clone());
			self.catalog.set_primary_key(primary_key.id, version, post);
			self.catalog.set_primary_key_shape(*shape, primary_key.id);
		}

		Ok(())
	}
}
