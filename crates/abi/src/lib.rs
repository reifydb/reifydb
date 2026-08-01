// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! `repr(C)` shapes crossing the host-extension FFI boundary. Types and constants only, no logic.
//!
//! Every type here is wire-stable: adding, removing, reordering or resizing a field breaks every extension linked
//! against an older version. The only version negotiation is the descriptor's `api` field, checked against
//! `CURRENT_API` at load time, plus `OPERATOR_ABI_TAG` for operators.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod callbacks;
pub mod catalog;
pub mod connector;
pub mod constants;
pub mod context;
pub mod data;
pub mod flow;
pub mod operator;
pub mod procedure;
pub mod transform;
