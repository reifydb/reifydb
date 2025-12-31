// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! OpenTelemetry/Jaeger tracing subsystem for ReifyDB.
//!
//! This crate provides OpenTelemetry integration for exporting traces from
//! ReifyDB's existing `tracing` instrumentation to OpenTelemetry collectors
//! like Jaeger, enabling distributed tracing visualization and analysis.
//!
//! # Features
//!
//! - OTLP exporter support (gRPC) - modern, recommended
//! - Jaeger native exporter support (UDP) - legacy
//! - Configurable sampling rates
//! - Batch export optimization
//! - Graceful shutdown with trace flushing
//!
//! # Architecture
//!
//! This subsystem works in conjunction with `sub-tracing`:
//! - `sub-tracing`: Manages console/file output via `tracing_subscriber`
//! - `sub-server-otel`: Adds OpenTelemetry export layer for external visualization
//!
//! Both contribute to the same `tracing` ecosystem, capturing spans from
//! the 221+ `#[instrument]` annotations throughout the codebase.
//!
//! # Integration with sub-tracing
//!
//! To fully integrate OpenTelemetry with the tracing subscriber, you need to
//! configure `sub-tracing` to include the OpenTelemetry layer. This requires
//! using the `with_layer()` method on `TracingBuilder` (see `sub-tracing`
//! documentation for details).
//!
//! The OpenTelemetry subsystem sets a global tracer provider that the
//! tracing-opentelemetry layer can use to export spans.
//!
//! # Example
//!
//! ```ignore
//! use reifydb::builder::ServerBuilder;
//! use reifydb_sub_server_otel::OtelConfig;
//!
//! // Step 1: Configure OpenTelemetry first (sets up global tracer provider)
//! let otel_config = OtelConfig::new()
//!     .service_name("my-reifydb")
//!     .endpoint("http://localhost:4317")
//!     .sample_ratio(1.0);
//!
//! // Step 2: Build database with OpenTelemetry subsystem
//! // Note: with_otel() must come BEFORE with_tracing()
//! let db = ServerBuilder::new(multi, single, cdc, eventbus)
//!     .with_otel(otel_config)
//!     .with_tracing(|t| t.with_filter("info"))
//!     .build()?;
//!
//! // Step 3: Manually add OpenTelemetry layer to tracing after start
//! // This is required because the subsystem pattern doesn't allow
//! // layer injection before the subscriber is initialized
//! // (See bin/testcontainer/src/main.rs for working example)
//! ```
//!
//! # Jaeger Setup
//!
//! To visualize traces in Jaeger:
//!
//! ```bash
//! # Run Jaeger all-in-one (includes collector + UI)
//! docker run -d --name jaeger \
//!   -e COLLECTOR_OTLP_ENABLED=true \
//!   -p 16686:16686 \
//!   -p 4317:4317 \
//!   -p 4318:4318 \
//!   jaegertracing/all-in-one:latest
//!
//! # Access Jaeger UI at http://localhost:16686
//! ```

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod config;
pub mod factory;
pub mod subsystem;

pub use config::{ExporterType, OtelConfig};
pub use factory::OtelSubsystemFactory;
pub use subsystem::OtelSubsystem;
