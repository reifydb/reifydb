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

	if let Some(address) = grpc {
		Namespace::Remote {
			id,
			name,
			parent_id,
			address,
		}
	} else {
		Namespace::Local {
			id,
			name,
			parent_id,
		}
	}
}
