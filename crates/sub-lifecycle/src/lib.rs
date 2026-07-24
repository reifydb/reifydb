// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Data lifecycle subsystem. Every actor that moves data along its lifecycle - from the commit buffer to the
//! persistent tier, through expiry, reclamation, and truncation - is registered here and runs on one lane, so
//! "what reclaims this, and which floor protects its readers?" has exactly one place to look.
//!
//! The subsystem is ALWAYS ON: it starts unconditionally at boot and every class registers declaratively. A class
//! with nothing to do reports zero work rather than not existing, because silently-absent maintenance is the defect
//! class this subsystem exists to eliminate.
//!
//! Retention rules live here; storage primitives do not. Components the store owns and calls on its own commit path
//! (`FlushEngine`, `CompactionEngine`, the operator-state scanner) stay in `reifydb-store-multi`; this crate owns the
//! executors that drive them on a schedule, and the traits lower layers implement to supply floors
//! (`QueryWatermark`, `EvictionWatermark`, `ListOperatorSettings`) stay with those layers so the dependency
//! direction points inward.

#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod actor;
pub mod cdc;
pub mod factory;
pub mod gc;
pub mod plane;
pub mod retention;
pub mod store;
pub mod subsystem;
