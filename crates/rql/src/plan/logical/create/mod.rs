// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod deferred;
pub mod dictionary;
pub mod event;
pub mod flow;
pub mod handler;
pub mod index;
pub mod namespace;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod ringbuffer;
pub mod series;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod tag;
pub mod transactional;

use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::ast::AstCreate,
	plan::logical::{Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create(
		&self,
		ast: AstCreate<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
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
			AstCreate::Policy(node) => self.compile_create_policy(node, tx),
			AstCreate::Procedure(node) => self.compile_create_procedure(node),
			AstCreate::Event(node) => self.compile_create_event(node),
			AstCreate::Tag(node) => self.compile_create_tag(node),
			AstCreate::Handler(node) => self.compile_create_handler(node),
		}
	}
}
