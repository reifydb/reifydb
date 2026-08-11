// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The extern-C procedure surface. A procedure may run RQL but keeps no operator state, so its context carries
//! no state or dictionary callbacks.

pub mod binding;
pub mod wire;
