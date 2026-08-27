// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod accumulator;
pub mod actor;
pub mod domains;
pub mod factory;
pub mod framework;
#[cfg(feature = "server")]
pub mod interceptor;
pub mod listener;
pub mod profiler;
pub mod sampler;
pub mod statement;
pub mod subsystem;
