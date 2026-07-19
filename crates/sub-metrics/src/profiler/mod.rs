// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Profiling subsystem. Bridges `reifydb-profiler` to the reifydb metrics pipeline: owns per-database
//! `ProfilerInstruments` (one duration histogram per category, registered as reporters into the per-database
//! `MetricsRegistry`), runs a single-writer `ProfilerCollectorActor` that folds incoming
//! batches into a transient `ProfilerAccumulator`, and exposes `ProfilerReader` for ad-hoc top-N reads. The producer
//! (the profiler layer's `EventBusSink`) emits `ProfilerScopeClosedEvent`/`ProfilerScopeBatchEvent` on scope close;
//! the listener forwards to the actor mailbox; the actor folds records into the accumulator off the hot path. Each
//! per-callsite `AggregateRecord` carries an embedded `PercentileHistogram` so p50/p90/p99 etc. are available
//! alongside the running totals. The accumulator is transient and read live by the per-category `::current` vtables.

pub mod accumulator;
pub mod actor;
pub mod builder;
pub mod factory;
pub mod instruments;
pub mod listener;
pub mod reader;
pub mod sink;
pub mod subsystem;
pub mod vtable;
