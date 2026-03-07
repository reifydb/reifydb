// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::sumtype::SumTypeId;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{HandlerId, NamespaceId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HandlerDef {
	pub id: HandlerId,
	pub namespace: NamespaceId,
	pub name: String,
	pub on_sumtype_id: SumTypeId,
	pub on_variant_tag: u8,
	pub body_source: String,
}
