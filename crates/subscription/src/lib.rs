// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Protocol-agnostic subscription consumption: the batching and delivery primitives transport-specific crates
//! build on, so backpressure and batching behave the same however the subscription is wired.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod batch;
pub mod delivery;
