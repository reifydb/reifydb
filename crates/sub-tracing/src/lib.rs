// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! In-process tracing subsystem built on `tracing_subscriber`. Configures the global subscriber, binds backends
//! (stdout, file, JSON), and exposes a builder so each deployment can pick the verbosity and format that fits.
//! External export to OTLP/Jaeger lives in `sub-server-otel` and stacks on top of this crate's subscriber.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod backend;
pub mod builder;
pub mod factory;
pub mod subsystem;
