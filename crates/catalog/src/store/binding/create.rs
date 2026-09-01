// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		binding::{Binding, BindingFormat, BindingProtocol},
		id::{NamespaceId, ProcedureId},
	},
	key::{catalog::BindingKey, namespace::NamespaceBindingKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore, Result,
	store::{
		binding::shape::{binding, binding_namespace},
		sequence::system::SystemSequence,
	},
};

pub struct BindingToCreate {
	pub namespace: NamespaceId,
	pub name: String,
	pub procedure: ProcedureId,
	pub protocol: BindingProtocol,
	pub format: BindingFormat,
}

impl CatalogStore {
	pub(crate) fn create_binding(txn: &mut AdminTransaction, to_create: BindingToCreate) -> Result<Binding> {
		let id = SystemSequence::next_binding_id(txn)?;

		let (protocol_str, http_method, http_path, rpc_name) = match &to_create.protocol {
			BindingProtocol::Http {
				method,
				path,
			} => ("http", method.as_str(), path.as_str(), ""),
			BindingProtocol::Grpc {
				name,
			} => ("grpc", "", "", name.as_str()),
			BindingProtocol::Ws {
				name,
			} => ("ws", "", "", name.as_str()),
		};

		let mut row = binding::allocate();
		binding::set_id(&mut row, u64::from(id));
		binding::set_namespace(&mut row, u64::from(to_create.namespace));
		binding::set_name(&mut row, &to_create.name);
		binding::set_procedure_id(&mut row, *to_create.procedure);
		binding::set_protocol(&mut row, protocol_str);
		binding::set_http_method(&mut row, http_method);
		binding::set_http_path(&mut row, http_path);
		binding::set_rpc_name(&mut row, rpc_name);
		binding::set_format(&mut row, to_create.format.as_str());

		txn.set(&BindingKey::encoded(id), row.freeze())?;

		let mut ns_row = binding_namespace::allocate();
		binding_namespace::set_id(&mut ns_row, u64::from(id));
		binding_namespace::set_name(&mut ns_row, &to_create.name);
		txn.set(&NamespaceBindingKey::encoded(to_create.namespace, id), ns_row.freeze())?;

		Ok(Binding {
			id,
			namespace: to_create.namespace,
			name: to_create.name,
			procedure_id: to_create.procedure,
			protocol: to_create.protocol,
			format: to_create.format,
		})
	}
}
