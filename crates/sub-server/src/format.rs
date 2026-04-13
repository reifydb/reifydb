// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Wire format for query/command/subscription responses.

use serde::{Deserialize, Serialize};

/// The three response formats the server can emit.
///
/// - `Json`  — rows-shape JSON: `[[{col: val, ...}, ...], ...]` (one inner array per frame)
/// - `Frames` — frames-shape JSON: `{frames: [ResponseFrame, ...]}` with columnar payloads
/// - `Rbcf`  — frames-shape binary, RBCF-encoded
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WireFormat {
	Json,
	Frames,
	Rbcf,
}
