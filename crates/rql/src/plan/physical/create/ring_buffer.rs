// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateRingBuffer;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	diagnostic::catalog::namespace_not_found,
	interface::{QueryTransaction, resolved::ResolvedNamespace},
};
use reifydb_type::return_error;

use crate::plan::{
	logical,
	physical::{Compiler, CreateRingBufferNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_ring_buffer<'a>(
		rx: &mut impl QueryTransaction,
		create: logical::CreateRingBufferNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		let Some(namespace_def) =
			CatalogStore::find_namespace_by_name(rx, create.ring_buffer.namespace.text())?
		else {
			return_error!(namespace_not_found(
				create.ring_buffer.namespace.clone(),
				create.ring_buffer.namespace.text()
			));
		};

		// Create a ResolvedNamespace
		let namespace_id = create.ring_buffer.namespace.clone();
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		Ok(CreateRingBuffer(CreateRingBufferNode {
			namespace: resolved_namespace,
			ring_buffer: create.ring_buffer.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			capacity: create.capacity,
		}))
	}
}
