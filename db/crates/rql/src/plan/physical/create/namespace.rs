// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical::CreateNamespaceNode,
	physical::{Compiler, CreateNamespacePlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_namespace<'a>(
		_rx: &mut impl QueryTransaction,
		create: CreateNamespaceNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// FIXME validate catalog
		Ok(PhysicalPlan::CreateNamespace(CreateNamespacePlan {
			namespace: create.namespace.name,
			if_not_exists: create.if_not_exists,
		}))
	}
}
