// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Subsystem trait surface: the contract every `sub-*` crate implements so the runtime supervisor can start, stop,
//! and health-check them through a uniform handle. The trait is intentionally minimal so a subsystem can hide its
//! own internal architecture while still participating in lifecycle management. New subsystems implement this trait
//! to be discoverable by the supervisor.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod subsystem;
