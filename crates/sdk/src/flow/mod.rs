// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Everything a flow is extended with: operators, which transform a change stream in place, and connectors,
//! which move rows between a flow and an external system.

pub mod connector;
pub mod extern_c;
pub mod operator;
