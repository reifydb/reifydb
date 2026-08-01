// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Producer side of the CDC stream: encodes committed deltas into CDC records, persists them, and advances the
//! publisher watermark so consumers can observe the new commit boundary.

pub mod producer;
pub mod watermark;
