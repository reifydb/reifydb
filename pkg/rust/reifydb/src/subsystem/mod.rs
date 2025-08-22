// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(feature = "sub_grpc")]
pub mod grpc;
mod subsystems;
pub mod worker_pool;
#[cfg(feature = "sub_ws")]
pub mod ws;

#[cfg(feature = "sub_grpc")]
pub use grpc::{GrpcSubsystem, GrpcSubsystemFactory};
#[cfg(feature = "sub_flow")]
pub use reifydb_sub_flow::{FlowSubsystem, FlowSubsystemFactory};
pub(crate) use subsystems::Subsystems;
pub use worker_pool::{WorkerPoolSubsystem, WorkerPoolSubsystemFactory};
#[cfg(feature = "sub_ws")]
pub use ws::{WsSubsystem, WsSubsystemFactory};

pub use crate::boot::Bootloader;
