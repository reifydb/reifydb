// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateRingBuffer;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	diagnostic::catalog::namespace_not_found, interface::QueryTransaction,
};
use reifydb_type::return_error;

use crate::plan::{
	logical::CreateRingBufferNode,
	physical::{Compiler, CreateRingBufferPlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_ring_buffer<'a>(
		rx: &mut impl QueryTransaction,
		create: CreateRingBufferNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		let Some(namespace) = CatalogStore::find_namespace_by_name(
			rx,
			create.ring_buffer.namespace.text(),
		)?
		else {
			return_error!(namespace_not_found(
				create.ring_buffer.namespace.clone(),
				create.ring_buffer.namespace.text()
			));
		};

		Ok(CreateRingBuffer(CreateRingBufferPlan {
			namespace,
			ring_buffer: create.ring_buffer.clone(),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			capacity: create.capacity,
		}))
	}
}
