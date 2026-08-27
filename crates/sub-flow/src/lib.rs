// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod builder;
pub(crate) mod catalog;
pub(crate) mod commit;
pub(crate) mod control;
pub(crate) mod discovery;
pub mod error;
pub mod operator;
pub(crate) mod progress;
pub mod subsystem;
