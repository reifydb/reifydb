// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod authentication;
pub(crate) mod binding;
pub mod column;
pub(crate) mod column_property;
pub mod column_snapshot;
pub(crate) mod config;
pub(crate) mod dictionary;
pub(crate) mod flow;
pub(crate) mod flow_edge;
pub(crate) mod granted_role;
pub(crate) mod handler;
pub(crate) mod identity;
pub(crate) mod identity_attribute;
pub(crate) mod identity_attribute_value;
pub(crate) mod migration;
pub(crate) mod namespace;
pub(crate) mod object;
pub(crate) mod operator;
pub mod operator_settings;
pub(crate) mod policy;
#[allow(dead_code)]
pub(crate) mod primary_key;
pub(crate) mod procedure;
#[allow(dead_code)]
pub mod queue;
pub(crate) mod relationship;
pub mod ringbuffer;
pub(crate) mod role;
pub mod row_settings;
pub(crate) mod row_shape;
pub(crate) mod sequence;
pub(crate) mod series;
pub(crate) mod sink;
pub(crate) mod source;
pub(crate) mod sumtype;
pub(crate) mod table;
pub(crate) mod time_source;
pub(crate) mod token;
pub mod view;

#[cfg(test)]
mod shape_fingerprints;
