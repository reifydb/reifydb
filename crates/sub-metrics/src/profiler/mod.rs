// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Profiling subsystem: bridges `reifydb-profiler` to the metrics pipeline. Scope-close events reach a
//! single-writer collector actor off the hot path, which folds them into a transient accumulator read live by the
//! per-category `::current` vtables. Instruments are per-database, so observations never blend across instances.

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
