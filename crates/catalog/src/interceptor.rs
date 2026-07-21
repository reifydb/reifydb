// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::config::{Config, ConfigKey},
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_transaction::{
	change::Change,
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor},
};
use reifydb_value::{byte_size::ByteSize, value::Value};

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

pub struct OperatorBudgetInterceptor {
	budget: OperatorStateBudgetHandle,
}

impl OperatorBudgetInterceptor {
	pub fn new(budget: OperatorStateBudgetHandle) -> Self {
		Self {
			budget,
		}
	}
}

impl PostCommitInterceptor for OperatorBudgetInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		if let Some(budget) = budget_from_config_changes(&ctx.changes.config) {
			self.budget.set_budget(budget);
		}
		Ok(())
	}
}

fn budget_from_config_changes(changes: &[Change<Config>]) -> Option<ByteSize> {
	changes.iter()
		.filter_map(|change| change.post.as_ref())
		.filter(|config| config.key == ConfigKey::OperatorStateMemoryLimit)
		.filter_map(|config| config_bytes(&config.value))
		.next_back()
		.map(ByteSize::from_bytes)
}

fn config_bytes(value: &Value) -> Option<u64> {
	match value {
		Value::Uint8(bytes) => Some(*bytes),
		Value::Any(inner) => config_bytes(inner),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::config::{Config, ConfigKey};
	use reifydb_transaction::change::{Change, OperationType};
	use reifydb_value::{byte_size::ByteSize, value::Value};

	use super::budget_from_config_changes;

	fn config_change(key: ConfigKey, value: Value) -> Change<Config> {
		Change {
			pre: None,
			post: Some(Config {
				key,
				value,
				default_value: Value::Uint8(0),
				description: "",
				requires_restart: false,
			}),
			op: OperationType::Update,
		}
	}

	#[test]
	fn picks_up_a_memory_limit_change() {
		// A SET CONFIG on the pool ceiling must reach the live pool through
		// the commit path; the interceptor pulls the new budget straight from
		// the committed change so nothing depends on the sampling loop running.
		let changes = vec![config_change(ConfigKey::OperatorStateMemoryLimit, Value::Uint8(512 * 1024 * 1024))];
		assert_eq!(budget_from_config_changes(&changes), Some(ByteSize::from_bytes(512 * 1024 * 1024)));
	}

	#[test]
	fn ignores_unrelated_config_changes() {
		// Only the operator-state ceiling drives the pool; a change to any
		// other config key must leave the budget untouched.
		let changes = vec![config_change(ConfigKey::QueryMemoryLimit, Value::Uint8(1))];
		assert_eq!(budget_from_config_changes(&changes), None);
	}

	#[test]
	fn unwraps_an_any_wrapped_value() {
		// Config rows round-trip through Value::Any; the extractor must see
		// through the wrapper the same way the config applier does, otherwise
		// a real SET CONFIG would silently fail to resize the pool.
		let changes = vec![config_change(
			ConfigKey::OperatorStateMemoryLimit,
			Value::Any(Box::new(Value::Uint8(64 * 1024 * 1024))),
		)];
		assert_eq!(budget_from_config_changes(&changes), Some(ByteSize::from_bytes(64 * 1024 * 1024)));
	}

	#[test]
	fn last_change_to_the_key_wins() {
		// Within one commit the most recent write is authoritative.
		let changes = vec![
			config_change(ConfigKey::OperatorStateMemoryLimit, Value::Uint8(1)),
			config_change(ConfigKey::OperatorStateMemoryLimit, Value::Uint8(2)),
		];
		assert_eq!(budget_from_config_changes(&changes), Some(ByteSize::from_bytes(2)));
	}

	#[test]
	fn no_matching_change_is_a_noop() {
		assert_eq!(budget_from_config_changes(&[]), None);
	}
}
