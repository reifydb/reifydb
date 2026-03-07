// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Flow compilation module - compiles RQL plans into Flows
//!
//! This module contains the flow compiler that was moved from reifydb-rql to avoid
//! lifetime issues with async recursion and generic MultiVersionCommandTransaction types.

pub mod compiler;
