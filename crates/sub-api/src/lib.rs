// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Subsystem trait surface: the contract every `sub-*` crate implements so the supervisor can start, stop and
//! health-check them through one handle. Deliberately minimal, so a subsystem can hide its own architecture and
//! still participate in lifecycle management.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod subsystem;
