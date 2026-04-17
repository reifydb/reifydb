//  SPDX-License-Identifier: Apache-2.0
//  Copyright (c) 2025 ReifyDB
#![cfg(not(reifydb_single_threaded))]

pub mod auth;
pub(crate) mod common;
pub mod grpc;
pub mod meta;
pub mod ws;
