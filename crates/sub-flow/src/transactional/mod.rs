// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Transactional (inline) view processing subsystem.
//!
//! Transactional views are updated synchronously within the same transaction
//! that writes to their source tables, providing immediate consistency.

pub mod interceptor;
pub mod registrar;
