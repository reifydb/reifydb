// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Source and sink connectors, moving rows between a flow and an external system. Both sides share one authoring
//! shape, so an extension implements either or both against a single set of primitives.

pub mod sink;
pub mod source;
