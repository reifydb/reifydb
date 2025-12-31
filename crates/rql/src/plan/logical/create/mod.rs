// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod deferred;
mod dictionary;
mod flow;
mod index;
mod namespace;
mod ringbuffer;
mod series;
mod table;
mod transactional;

use reifydb_transaction::IntoStandardTransaction;

use crate::{
	ast::AstCreate,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) async fn compile_create<T: IntoStandardTransaction>(
		&self,
		ast: AstCreate,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		match ast {
			AstCreate::DeferredView(node) => self.compile_deferred_view(node, tx).await,
			AstCreate::TransactionalView(node) => self.compile_transactional_view(node, tx).await,
			AstCreate::Flow(node) => self.compile_create_flow(node, tx).await,
			AstCreate::Namespace(node) => self.compile_create_namespace(node),
			AstCreate::Series(node) => self.compile_create_series(node),
			AstCreate::Table(node) => self.compile_create_table(node, tx).await,
			AstCreate::RingBuffer(node) => self.compile_create_ringbuffer(node, tx).await,
			AstCreate::Dictionary(node) => self.compile_create_dictionary(node),
			AstCreate::Index(node) => self.compile_create_index(node),
		}
	}
}
