// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The extern-C transform surface. A transform is a pure function over columns, so its context carries neither
//! state nor dictionary callbacks.

pub mod binding;
pub mod wire;
