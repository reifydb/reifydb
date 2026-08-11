// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! wire holds the repr(C) contract both sides of the boundary agree on byte for byte; binding holds the
//! plain-Rust helpers written against it.

pub mod binding;
pub mod wire;
