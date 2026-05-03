// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! OpenTelemetry / Jaeger tracing exporter. Configures a tracing subscriber that sends spans emitted by the rest of
//! the workspace to a collector over OTLP. The crate adds the export pipeline; in-process tracing is owned by
//! `sub-tracing`. Deployments that do not want external tracing simply do not enable this subsystem.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod config;
#[cfg(not(reifydb_single_threaded))]
pub mod factory;
#[cfg(not(reifydb_single_threaded))]
pub mod subsystem;
