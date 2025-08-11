// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Adapters for integrating existing subsystems with the Subsystem trait
//!
//! This module provides adapters that wrap existing subsystem implementations
//! to make them compatible with the unified Subsystem trait.

pub mod flow;
#[cfg(any(feature = "server", feature = "grpc"))]
pub mod grpc;
#[cfg(any(feature = "server", feature = "websocket"))]
pub mod websocket;

pub use flow::FlowSubsystemAdapter;
#[cfg(any(feature = "server", feature = "grpc"))]
pub use grpc::GrpcSubsystemAdapter;
#[cfg(any(feature = "server", feature = "websocket"))]
pub use websocket::WsSubsystemAdapter;