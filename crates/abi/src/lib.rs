// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

//! C ABI definitions for ReifyDB FFI operators
//!
//! This crate provides the stable C ABI interface that FFI operators must implement.
//! It defines FFI-safe types and function signatures for operators to interact with
//! the ReifyDB host system.

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod callbacks;
pub mod catalog;
pub mod constants;
pub mod context;
pub mod data;
pub mod flow;
pub mod operator;
