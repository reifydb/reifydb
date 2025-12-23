// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(warnings))]

//! Subsystem API crate providing common interfaces for ReifyDB subsystems
//!
//! This crate contains the core traits and types that all subsystems must implement
//! and use to interact with the ReifyDB system.

pub mod subsystem;

pub use subsystem::{HealthStatus, Subsystem, SubsystemFactory};
