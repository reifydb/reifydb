// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Always-on profiler primitives over `tracing`: `ProfilerLayer` appends a fixed-size `MinimalSpanRecord` per
//! matching span without allocating on the hot path, and `ScopeHandle::finish` drains them to a `ProfilerSink`.
//!
//! Deliberately free of metric and IoC dependencies, so tests can embed the layer with a `NoopSink` and the data
//! model is usable without pulling in the subsystem.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod callsite;
pub mod category;
pub mod event;
pub mod format;
pub mod intern;
pub mod layer;
pub mod percentile;
pub mod record;
pub mod scope;
pub mod sink;
pub mod summary;
pub mod visit;
