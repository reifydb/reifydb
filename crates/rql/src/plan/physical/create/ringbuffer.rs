// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateRingBuffer;
use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
	plan::{
		logical,
		physical::{Compiler, CreateRingBufferNode, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_ringbuffer(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateRingBufferNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		// Get namespace name from the MaybeQualified type (join all segments for nested namespaces)
		let ns_segments: Vec<&str> = create.ringbuffer.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.ringbuffer.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return_error!(namespace_not_found(ns_fragment, &ns_segments.join("::")));
		};

		let namespace_id = if let Some(n) = create.ringbuffer.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);

		Ok(CreateRingBuffer(CreateRingBufferNode {
			namespace: resolved_namespace,
			ringbuffer: self.interner.intern_fragment(&create.ringbuffer.name),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			capacity: create.capacity,
			partition_by: create.partition_by,
			ttl: create.ttl,
		}))
	}
}
