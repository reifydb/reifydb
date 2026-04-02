// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

//! ReifyDB Operator SDK

#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod catalog;
pub mod connector;
pub mod error;
pub mod ffi;
pub mod flow;
pub mod marshal;
pub mod operator;
pub mod procedure;
pub mod rql;
pub mod state;
pub mod store;
pub mod testing;
pub mod transform;
