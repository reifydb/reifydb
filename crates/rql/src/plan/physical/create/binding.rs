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
				fragment: Fragment::internal(proc_name.to_string()),
			}
			.into());
		};

		let (protocol, default_format) = match create.protocol.kind {
			AstBindingProtocolKind::Http => {
				let method_str = create
					.protocol
					.method
					.as_ref()
					.ok_or_else(|| CatalogError::NotFound {
						kind: CatalogObjectKind::Procedure,
						namespace: ns_segments.join("::"),
						name: "HTTP binding requires `method`".to_string(),
						fragment: Fragment::internal("method".to_string()),
					})?
					.text()
					.to_ascii_uppercase();
				let method = HttpMethod::parse(&method_str).ok_or_else(|| CatalogError::NotFound {
					kind: CatalogObjectKind::Procedure,
					namespace: ns_segments.join("::"),
					name: format!(
						"unknown HTTP method '{}': expected GET, POST, PUT, PATCH, or DELETE",
						method_str
					),
					fragment: Fragment::internal(method_str.clone()),
				})?;
				let path = create
					.protocol
					.path
					.as_ref()
					.ok_or_else(|| CatalogError::NotFound {
						kind: CatalogObjectKind::Procedure,
						namespace: ns_segments.join("::"),
						name: "HTTP binding requires `path`".to_string(),
						fragment: Fragment::internal("path".to_string()),
					})?
					.text()
					.to_string();
				(
					BindingProtocol::Http {
						method,
						path,
					},
					BindingFormat::Json,
				)
			}
			AstBindingProtocolKind::Grpc => {
				let rpc_name = create
					.protocol
					.rpc_name
					.as_ref()
					.ok_or_else(|| CatalogError::NotFound {
						kind: CatalogObjectKind::Procedure,
						namespace: ns_segments.join("::"),
						name: "GRPC binding requires `name`".to_string(),
						fragment: Fragment::internal("name".to_string()),
					})?
					.text()
					.to_string();
				(
					BindingProtocol::Grpc {
						name: rpc_name,
					},
					BindingFormat::Rbcf,
				)
			}
			AstBindingProtocolKind::Ws => {
				let rpc_name = create
					.protocol
					.rpc_name
					.as_ref()
					.ok_or_else(|| CatalogError::NotFound {
						kind: CatalogObjectKind::Procedure,
						namespace: ns_segments.join("::"),
						name: "WS binding requires `name`".to_string(),
						fragment: Fragment::internal("name".to_string()),
					})?
					.text()
					.to_string();
				(
					BindingProtocol::Ws {
						name: rpc_name,
					},
					BindingFormat::Json,
				)
			}
		};

		let format = if let Some(fmt_frag) = create.protocol.format.as_ref() {
			let fmt_str = fmt_frag.text().to_ascii_lowercase();
			BindingFormat::parse(&fmt_str).ok_or_else(|| CatalogError::NotFound {
				kind: CatalogObjectKind::Procedure,
				namespace: ns_segments.join("::"),
				name: format!("unknown binding format '{}': expected json, frames, or rbcf", fmt_str),
				fragment: Fragment::internal(fmt_str.clone()),
			})?
		} else {
			default_format
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
