// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod deferred;
mod dictionary;
mod flow;
mod index;
mod namespace;
mod ringbuffer;
mod series;
mod table;
mod transactional;

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreate,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) async fn compile_create<'a, T>(ast: AstCreate<'a>, tx: &'a mut T) -> crate::Result<LogicalPlan<'a>>
	where
		T: CatalogQueryTransaction,
	{
		match ast {
			AstCreate::DeferredView(node) => Self::compile_deferred_view(node, tx).await,
			AstCreate::TransactionalView(node) => Self::compile_transactional_view(node, tx).await,
			AstCreate::Flow(node) => Self::compile_create_flow(node, tx).await,
			AstCreate::Namespace(node) => Self::compile_create_namespace(node, tx),
			AstCreate::Series(node) => Self::compile_create_series(node, tx),
			AstCreate::Table(node) => Self::compile_create_table(node, tx).await,
			AstCreate::RingBuffer(node) => Self::compile_create_ringbuffer(node, tx).await,
			AstCreate::Dictionary(node) => Self::compile_create_dictionary(node, tx),
			AstCreate::Index(node) => Self::compile_create_index(node, tx),
		}
	}
}
