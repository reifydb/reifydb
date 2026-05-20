// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::sumtype::VariantRef;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{HandlerId, NamespaceId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Handler {
	pub id: HandlerId,
	pub namespace: NamespaceId,
	pub name: String,
	pub variant: VariantRef,
	pub body_source: String,
}
