// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod client;
#[cfg(all(feature = "dst", reifydb_single_threaded))]
pub mod dst;
pub mod generated;

pub use client::{GrpcClient, GrpcSubscription};
#[cfg(all(feature = "dst", reifydb_single_threaded))]
pub use dst::DstGrpcClient;
