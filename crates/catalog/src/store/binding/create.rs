// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		binding::{Binding, BindingFormat, BindingProtocol},
		id::{NamespaceId, ProcedureId},
	},
	key::{binding::BindingKey, namespace_binding::NamespaceBindingKey},
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

		let mut row = binding::SHAPE.allocate();
		binding::SHAPE.set_u64(&mut row, binding::ID, id);
		binding::SHAPE.set_u64(&mut row, binding::NAMESPACE, to_create.namespace);
		binding::SHAPE.set_utf8(&mut row, binding::NAME, &to_create.name);
		binding::SHAPE.set_u64(&mut row, binding::PROCEDURE_ID, *to_create.procedure);
		binding::SHAPE.set_utf8(&mut row, binding::PROTOCOL, protocol_str);
		binding::SHAPE.set_utf8(&mut row, binding::HTTP_METHOD, http_method);
		binding::SHAPE.set_utf8(&mut row, binding::HTTP_PATH, http_path);
		binding::SHAPE.set_utf8(&mut row, binding::RPC_NAME, rpc_name);
		binding::SHAPE.set_utf8(&mut row, binding::FORMAT, to_create.format.as_str());
		binding::SHAPE.set_u8(&mut row, binding::ENABLED, 1u8);

		txn.set(&BindingKey::encoded(id), row)?;

		let mut ns_row = binding_namespace::SHAPE.allocate();
		binding_namespace::SHAPE.set_u64(&mut ns_row, binding_namespace::ID, id);
		binding_namespace::SHAPE.set_utf8(&mut ns_row, binding_namespace::NAME, &to_create.name);
		txn.set(&NamespaceBindingKey::encoded(to_create.namespace, id), ns_row)?;

		Ok(Binding {
			id,
			namespace: to_create.namespace,
			name: to_create.name,
			procedure_id: to_create.procedure,
			protocol: to_create.protocol,
			format: to_create.format,
			enabled: true,
		})
	}
}
