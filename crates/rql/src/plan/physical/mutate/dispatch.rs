// SPDX-License-Identifier: AGPL-3.0-or-later
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
		let ns_name = if dispatch.on_event.namespace.is_empty() {
			"default".to_string()
		} else {
			dispatch.on_event.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join("::")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &ns_name)? else {
			let ns_fragment = Fragment::internal(ns_name.clone());
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: ns_name.to_string(),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Look up event sumtype by name
		let event_name = dispatch.on_event.name.text();
		let Some(sumtype_def) = self.catalog.find_sumtype_by_name(rx, namespace_def.id, event_name)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Event,
				namespace: ns_name.to_string(),
				name: event_name.to_string(),
				fragment: Fragment::internal(event_name.to_string()),
			}
			.into());
		};

		if sumtype_def.kind != SumTypeKind::Event {
			return Err(internal_error!("'{}' is not an EVENT type", event_name));
		}

		// Convert fields - variant resolved at runtime
		let fields = dispatch.fields.into_iter().map(|(name, expr)| (name.text().to_string(), expr)).collect();

		Ok(PhysicalPlan::Dispatch(DispatchNode {
			namespace: namespace_def,
			on_sumtype_id: sumtype_def.id,
			variant_name: dispatch.variant.text().to_string(),
			fields,
		}))
	}
}
