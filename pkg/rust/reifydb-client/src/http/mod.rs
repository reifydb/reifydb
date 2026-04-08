// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
mod client;
#[cfg(all(feature = "dst", reifydb_single_threaded))]
pub mod dst;

pub use client::HttpClient;
#[cfg(all(feature = "dst", reifydb_single_threaded))]
pub use dst::DstHttpClient;
