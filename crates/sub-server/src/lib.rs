// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod accept;
pub mod actor;
pub mod auth;
pub mod binding;
pub mod claim;
pub mod dispatch;
pub mod execute;
pub mod format;
pub mod interceptor;
pub mod response;
pub mod state;
pub mod subscription;
pub mod wire;
