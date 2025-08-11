// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Adapters for integrating existing subsystems with the Subsystem trait
//!
//! This module provides adapters that wrap existing subsystem implementations
//! to make them compatible with the unified Subsystem trait.

pub mod flow;

pub use flow::FlowSubsystemAdapter;