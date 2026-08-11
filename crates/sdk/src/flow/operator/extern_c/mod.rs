// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The extern-C operator surface: the symbols a cdylib exports, the context the host hands each call, and the
//! callback tables an operator may reach.

pub mod binding;
pub mod wire;
