// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Always-on profiler primitives. Builds on `tracing` to capture per-scope span records without allocating on the hot
//! path. `ProfilerLayer` is a `tracing_subscriber::Layer` that intercepts spans matching a curated set of
//! `ProfilerCategory` prefixes, extracts numeric fields through reusable thread-local visitors, and appends a
//! fixed-size `MinimalSpanRecord` to scope-local state. `ProfilerScope::start` opens a scope; `ScopeHandle::finish`
//! drains the accumulated records, builds a `ProfilerSummary` for the caller, and hands the batch to a `ProfilerSink`
//! for downstream delivery (in production this is `sub-profiler`'s EventBus bridge).
//!
//! This crate stays free of any metric or IoC dependency so the layer can be embedded in tests with a `NoopSink` and
//! so the rest of the workspace can use the data model without pulling in the subsystem.

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
