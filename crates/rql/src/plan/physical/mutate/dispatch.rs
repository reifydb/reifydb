// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{interface::catalog::sumtype::SumTypeKind, internal_error};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	nodes::DispatchNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_dispatch(
		&mut self,
		rx: &mut Transaction<'_>,
		dispatch: logical::DispatchNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		// Resolve namespace
		let ns_segments: Vec<&str> = dispatch.on_event.namespace.iter().map(|n| n.text()).collect();
		let ns_name: String = if ns_segments.is_empty() {
			"default".to_string()
		} else {
			ns_segments.join("::")
		};
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = Fragment::internal(ns_name.clone());
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: ns_name,
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Look up event sumtype by name
		let event_name = dispatch.on_event.name.text();
		let Some(sumtype) = self.catalog.find_sumtype_by_name(rx, namespace.id(), event_name)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Event,
				namespace: ns_name.clone(),
				name: event_name.to_string(),
				fragment: Fragment::internal(event_name.to_string()),
			}
			.into());
		};

		if sumtype.kind != SumTypeKind::Event {
			return Err(internal_error!("'{}' is not an EVENT type", event_name));
		}

		// Convert fields - variant resolved at runtime
		let fields = dispatch.fields.into_iter().map(|(name, expr)| (name.text().to_string(), expr)).collect();

		Ok(PhysicalPlan::Dispatch(DispatchNode {
			namespace,
			on_sumtype_id: sumtype.id,
			variant_name: dispatch.variant.text().to_string(),
			fields,
		}))
	}
}
