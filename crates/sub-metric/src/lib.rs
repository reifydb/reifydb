// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Metric collection subsystem: watches the engine's metric registry, samples it on a cadence, and delivers
//! snapshots to whatever sink the deployment has configured (an in-process listener, a periodic logger, an external
//! exporter via the OTel subsystem). Interceptors hook into individual metric updates so a sink can react to events
//! rather than poll.
//!
//! The crate produces no metrics of its own; the engine's `metric/` crate is the source. This subsystem only owns
//! the delivery path. New sinks plug in as listeners; new metric kinds belong in `metric/`.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod actor;
pub mod factory;
pub mod interceptor;
pub mod listener;
pub mod subsystem;
