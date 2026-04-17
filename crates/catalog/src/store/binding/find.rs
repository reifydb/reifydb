// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		binding::{Binding, BindingFormat, BindingProtocol, HttpMethod},
		id::{BindingId, NamespaceId, ProcedureId},
	},
	key::{binding::BindingKey, namespace_binding::NamespaceBindingKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::binding::shape::{binding, binding_namespace},
};

impl CatalogStore {
	pub(crate) fn find_binding(rx: &mut Transaction<'_>, id: BindingId) -> Result<Option<Binding>> {
		let Some(multi) = rx.get(&BindingKey::encoded(id))? else {
			return Ok(None);
		};
		Ok(Some(decode_binding(&multi.row)))
	}

	pub(crate) fn find_binding_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<Binding>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceBindingKey::full_scan(namespace), 1024)?;

		let mut found_id = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let row = &multi.row;
			let bound_name = binding_namespace::SHAPE.get_utf8(row, binding_namespace::NAME);
			if name == bound_name {
				found_id =
					Some(BindingId(binding_namespace::SHAPE.get_u64(row, binding_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(id) = found_id else {
			return Ok(None);
		};

		Self::find_binding(rx, id)
	}
}

pub(crate) fn decode_binding(row: &EncodedRow) -> Binding {
	let id = BindingId(binding::SHAPE.get_u64(row, binding::ID));
	let namespace = NamespaceId(binding::SHAPE.get_u64(row, binding::NAMESPACE));
	let name = binding::SHAPE.get_utf8(row, binding::NAME).to_string();
	let procedure_id = ProcedureId::from_raw(binding::SHAPE.get_u64(row, binding::PROCEDURE_ID));
	let protocol_str = binding::SHAPE.get_utf8(row, binding::PROTOCOL);
	let format_str = binding::SHAPE.get_utf8(row, binding::FORMAT);
	let enabled = binding::SHAPE.get_u8(row, binding::ENABLED) != 0;

	let protocol = match protocol_str {
		"http" => {
			let method_str = binding::SHAPE.get_utf8(row, binding::HTTP_METHOD);
			let path = binding::SHAPE.get_utf8(row, binding::HTTP_PATH).to_string();
			BindingProtocol::Http {
				method: HttpMethod::parse(method_str).unwrap_or(HttpMethod::Get),
				path,
			}
		}
		"grpc" => {
			let rpc_name = binding::SHAPE.get_utf8(row, binding::RPC_NAME).to_string();
			BindingProtocol::Grpc {
				name: rpc_name,
			}
		}
		_ => {
			let rpc_name = binding::SHAPE.get_utf8(row, binding::RPC_NAME).to_string();
			BindingProtocol::Ws {
				name: rpc_name,
			}
		}
	};

	let format = BindingFormat::parse(format_str).unwrap_or(BindingFormat::Frames);

	Binding {
		id,
		namespace,
		name,
		procedure_id,
		protocol,
		format,
		enabled,
	}
}
