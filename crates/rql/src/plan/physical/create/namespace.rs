// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, CreateNamespaceNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_namespace(
		_rx: &mut impl QueryTransaction,
		create: logical::CreateNamespaceNode,
	) -> crate::Result<PhysicalPlan> {
		// FIXME validate catalog
		Ok(PhysicalPlan::CreateNamespace(CreateNamespaceNode {
			namespace: create.namespace,
			if_not_exists: create.if_not_exists,
		}))
	}
}
