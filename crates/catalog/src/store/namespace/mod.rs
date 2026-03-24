// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	catalog::{id::NamespaceId, namespace::Namespace},
	store::MultiVersionValues,
};

use crate::store::namespace::schema::namespace;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub mod schema;
pub mod update;

pub(crate) fn convert_namespace(multi: MultiVersionValues) -> Namespace {
	let row = multi.values;
	let id = NamespaceId(namespace::SCHEMA.get_u64(&row, namespace::ID));
	let name = namespace::SCHEMA.get_utf8(&row, namespace::NAME).to_string();
	let parent_id = NamespaceId(namespace::SCHEMA.get_u64(&row, namespace::PARENT_ID));
	let grpc =
		namespace::SCHEMA.try_get_utf8(&row, namespace::GRPC).map(|s| s.to_string()).filter(|s| !s.is_empty());
	let local_name = namespace::SCHEMA
		.try_get_utf8(&row, namespace::LOCAL_NAME)
		.filter(|s| !s.is_empty())
		.unwrap_or_else(|| name.rsplit_once("::").map(|(_, s)| s).unwrap_or(&name))
		.to_string();

	if let Some(address) = grpc {
		let token = namespace::SCHEMA
			.try_get_utf8(&row, namespace::TOKEN)
			.map(|s| s.to_string())
			.filter(|s| !s.is_empty());
		Namespace::Remote {
			id,
			name,
			local_name,
			parent_id,
			address,
			token,
		}
	} else {
		Namespace::Local {
			id,
			name,
			local_name,
			parent_id,
		}
	}
}
