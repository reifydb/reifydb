// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{
	catalog::{id::NamespaceId, namespace::Namespace},
	store::MultiVersionRow,
};

use crate::store::namespace::shape::namespace;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub mod shape;
pub mod update;

pub(crate) fn convert_namespace(multi: MultiVersionRow) -> Namespace {
	let bytes = multi.bytes;
	let id = NamespaceId(namespace::get_id(&bytes));
	let name = namespace::get_name(&bytes).to_string();
	let parent_id = NamespaceId(namespace::get_parent_id(&bytes));
	let grpc = namespace::try_get_grpc(&bytes).map(|s| s.to_string()).filter(|s| !s.is_empty());
	let local_name = namespace::try_get_local_name(&bytes)
		.filter(|s| !s.is_empty())
		.unwrap_or_else(|| name.rsplit_once("::").map(|(_, s)| s).unwrap_or(&name))
		.to_string();

	if let Some(address) = grpc {
		let token = namespace::try_get_token(&bytes).map(|s| s.to_string()).filter(|s| !s.is_empty());
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
