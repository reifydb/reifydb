// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Data lifecycle subsystem. Every actor that moves data along its lifecycle runs on one lane and starts
//! unconditionally, so a class with nothing to do reports zero work rather than not existing. Retention rules live
//! here; the store-owned primitives and the floor traits stay with the layers that own them, pointing the
//! dependency direction inward.

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
