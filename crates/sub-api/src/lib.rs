// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

//! Subsystem API crate providing common interfaces for ReifyDB subsystems
//!
//! This crate contains the core traits and types that all subsystems must implement
//! and use to interact with the ReifyDB system.

pub mod subsystem;
