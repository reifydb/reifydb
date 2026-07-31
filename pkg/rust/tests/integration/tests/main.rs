// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

mod auth;
mod catalog;
mod dictionary_durability;
mod export;
mod flow;
mod identity_kind;
mod metric;
mod persistence;
mod queue;
mod storage;
mod store;
mod system;
mod virtual_table;
mod wire_format;
