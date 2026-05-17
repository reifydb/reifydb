// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Profiling subsystem. Bridges `reifydb-profiler` to the reifydb metric pipeline: registers six static histograms
//! (one per category) with `STATIC_REGISTRY`, runs a single-writer `ProfileCollectorActor` that folds incoming
//! batches into a transient `ProfileAccumulator`, and exposes `ProfilerReader` for ad-hoc top-N reads. The producer
//! (the profiler layer's `EventBusSink`) emits `ProfileScopeClosedEvent`/`ProfileScopeBatchEvent` on scope close;
//! the listener forwards to the actor mailbox; the actor folds records into the accumulator off the hot path. Each
//! per-callsite `AggregateRecord` carries an embedded `PercentileHistogram` so p50/p90/p99 etc. are available
//! alongside the running totals. The accumulator is transient: long-term storage is the metric subsystem's
//! responsibility.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod accumulator;
pub mod actor;
pub mod builder;
pub mod factory;
pub mod histograms;
pub mod listener;
pub mod reader;
pub mod sink;
pub mod subsystem;
