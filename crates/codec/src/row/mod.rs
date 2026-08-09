// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The row envelope family types: the opaque `EncodedBytes` a flat store hands back, the shape
//! machinery that gives it a typed view, and one submodule per storage family that owns a header
//! layout of its own. A family's carve lands here as its own submodule.

pub mod bytes;
pub mod catalog;
pub mod dictionary;
pub mod le;
pub mod operator;
pub mod shape;
