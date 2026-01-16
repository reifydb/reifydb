// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod deferred;
pub mod dictionary;
pub mod flow;
pub mod index;
pub mod namespace;
pub mod ringbuffer;
pub mod series;
pub mod subscription;
pub mod table;
pub mod transactional;

use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{
	ast::ast::AstCreate,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create<T: IntoStandardTransaction>(
		&self,
		ast: AstCreate,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		match ast {
			AstCreate::DeferredView(node) => self.compile_deferred_view(node, tx),
			AstCreate::TransactionalView(node) => self.compile_transactional_view(node, tx),
			AstCreate::Flow(node) => self.compile_create_flow(node, tx),
			AstCreate::Namespace(node) => self.compile_create_namespace(node),
			AstCreate::Series(node) => self.compile_create_series(node),
			AstCreate::Table(node) => self.compile_create_table(node, tx),
			AstCreate::RingBuffer(node) => self.compile_create_ringbuffer(node, tx),
			AstCreate::Dictionary(node) => self.compile_create_dictionary(node),
			AstCreate::Index(node) => self.compile_create_index(node),
			AstCreate::Subscription(node) => self.compile_create_subscription(node),
		}
	}
}
