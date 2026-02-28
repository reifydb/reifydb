// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod deferred;
pub mod dictionary;
pub mod event;
pub mod flow;
pub mod handler;
pub mod index;
pub mod migration;
pub mod namespace;
pub mod primary_key;
pub mod procedure;
pub mod property;
pub mod ringbuffer;
pub mod series;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod tag;
pub mod transactional;

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::ast::AstCreate,
	plan::logical::{
		Compiler, CreateAuthenticationNode, CreatePolicyNode, CreateRoleNode, CreateUserNode, LogicalPlan,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create(
		&self,
		ast: AstCreate<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		match ast {
			AstCreate::DeferredView(node) => self.compile_deferred_view(node, tx),
			AstCreate::TransactionalView(node) => self.compile_transactional_view(node, tx),
			AstCreate::Flow(node) => self.compile_create_flow(node, tx),
			AstCreate::Namespace(node) => self.compile_create_namespace(node),
			AstCreate::Series(node) => self.compile_create_series(node, tx),
			AstCreate::Table(node) => self.compile_create_table(node, tx),
			AstCreate::RingBuffer(node) => self.compile_create_ringbuffer(node, tx),
			AstCreate::Dictionary(node) => self.compile_create_dictionary(node),
			AstCreate::Enum(node) => self.compile_create_sumtype(node),
			AstCreate::Index(node) => self.compile_create_index(node),
			AstCreate::Subscription(node) => self.compile_create_subscription(node, tx),
			AstCreate::PrimaryKey(node) => self.compile_create_primary_key(node, tx),
			AstCreate::ColumnProperty(node) => self.compile_create_column_property(node, tx),
			AstCreate::Procedure(node) => self.compile_create_procedure(node),
			AstCreate::Event(node) => self.compile_create_event(node),
			AstCreate::Tag(node) => self.compile_create_tag(node),
			AstCreate::Handler(node) => self.compile_create_handler(node),
			AstCreate::User(node) => Ok(LogicalPlan::CreateUser(CreateUserNode {
				name: node.name,
			})),
			AstCreate::Authentication(node) => {
				Ok(LogicalPlan::CreateAuthentication(CreateAuthenticationNode {
					user: node.user,
					entries: node.entries,
				}))
			}
			AstCreate::Role(node) => Ok(LogicalPlan::CreateRole(CreateRoleNode {
				name: node.name,
			})),
			AstCreate::Policy(node) => Ok(LogicalPlan::CreatePolicy(CreatePolicyNode {
				name: node.name,
				target_type: node.target_type,
				scope: node.scope,
				operations: node.operations,
			})),
			AstCreate::Migration(node) => self.compile_create_migration(node),
		}
	}
}
