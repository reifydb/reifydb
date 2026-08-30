// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod parser;
pub mod storage;

use serde::{Deserialize, Serialize};

use crate::interface::catalog::object::ObjectId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricsId {
	Object(ObjectId),

	System,
}
