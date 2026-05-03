// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Protocol-agnostic subscription consumption: batching of incoming change events and the delivery primitives that
//! transport-specific subscription crates build on top of. Anything that wants to receive a stream of changes
//! (transport handlers, in-process listeners, replication consumers) shapes its consumer side around the types in
//! this crate so backpressure and batching behave consistently regardless of how the subscription is wired.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod batch;
pub mod delivery;
