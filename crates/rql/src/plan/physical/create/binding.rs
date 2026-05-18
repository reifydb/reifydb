// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::interface::catalog::binding::{BindingFormat, BindingProtocol, HttpMethod};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	ast::ast::AstBindingProtocolKind,
	nodes::CreateBindingNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_binding(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateBindingNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = create.name.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: ns_segments.join("::"),
				name: String::new(),
				fragment: Fragment::internal(ns_segments.join("::")),
			}
			.into());
		};

		let proc_ns_segments: Vec<&str> = if create.procedure.namespace.is_empty() {
			ns_segments.clone()
		} else {
			create.procedure.namespace.iter().map(|n| n.text()).collect()
		};
		let Some(proc_ns) = self.catalog.find_namespace_by_segments(rx, &proc_ns_segments)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: proc_ns_segments.join("::"),
				name: String::new(),
				fragment: Fragment::internal(proc_ns_segments.join("::")),
			}
			.into());
		};

		let proc_name = create.procedure.name.text();
		let Some(procedure) = self.catalog.find_procedure_by_name(rx, proc_ns.id(), proc_name)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Procedure,
				namespace: proc_ns_segments.join("::"),
				name: proc_name.to_string(),
				fragment: Fragment::internal(proc_name),
			}
			.into());
		};

		let protocol = match create.protocol.kind {
			AstBindingProtocolKind::Http => {
				let method_frag = create.protocol.method.as_ref().ok_or_else(|| {
					CatalogError::InvalidBindingConfig {
						reason: "HTTP binding requires `method`".to_string(),
						fragment: Fragment::internal("method"),
					}
				})?;
				let method_str = method_frag.text().to_ascii_uppercase();
				let method = HttpMethod::parse(&method_str).ok_or_else(|| {
					CatalogError::InvalidBindingConfig {
						reason: format!(
							"unknown HTTP method '{}': expected GET, POST, PUT, PATCH, or DELETE",
							method_str
						),
						fragment: method_frag.to_owned(),
					}
				})?;
				let path_frag = create.protocol.path.as_ref().ok_or_else(|| {
					CatalogError::InvalidBindingConfig {
						reason: "HTTP binding requires `path`".to_string(),
						fragment: Fragment::internal("path"),
					}
				})?;
				let path = path_frag.text().to_string();
				if !path.starts_with('/') {
					return Err(CatalogError::InvalidBindingConfig {
						reason: format!("HTTP path must start with `/`, got `{}`", path),
						fragment: path_frag.to_owned(),
					}
					.into());
				}
				BindingProtocol::Http {
					method,
					path,
				}
			}
			AstBindingProtocolKind::Grpc => {
				let rpc_name_frag = create.protocol.rpc_name.as_ref().ok_or_else(|| {
					CatalogError::InvalidBindingConfig {
						reason: "gRPC binding requires `name`".to_string(),
						fragment: Fragment::internal("name"),
					}
				})?;
				BindingProtocol::Grpc {
					name: rpc_name_frag.text().to_string(),
				}
			}
			AstBindingProtocolKind::Ws => {
				let rpc_name_frag = create.protocol.rpc_name.as_ref().ok_or_else(|| {
					CatalogError::InvalidBindingConfig {
						reason: "WS binding requires `name`".to_string(),
						fragment: Fragment::internal("name"),
					}
				})?;
				BindingProtocol::Ws {
					name: rpc_name_frag.text().to_string(),
				}
			}
		};

		let format = if let Some(fmt_frag) = create.protocol.format.as_ref() {
			let fmt_str = fmt_frag.text().to_ascii_lowercase();
			BindingFormat::parse(&fmt_str).ok_or_else(|| CatalogError::InvalidBindingConfig {
				reason: format!("unknown binding format '{}': expected json, frames, or rbcf", fmt_str),
				fragment: fmt_frag.to_owned(),
			})?
		} else {
			BindingFormat::Frames
		};

		Ok(PhysicalPlan::CreateBinding(CreateBindingNode {
			namespace,
			name: self.interner.intern_fragment(&create.name.name),
			procedure_id: procedure.id(),
			protocol,
			format,
		}))
	}
}
