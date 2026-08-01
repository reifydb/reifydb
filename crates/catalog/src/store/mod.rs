// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Storage-facing catalog persistence: one sibling module per object kind, owning its `EncodedKey`
//! layout and its (de)serialisation. `catalog/` goes through this layer instead of a backend
//! directly, so a new backend re-implements this surface rather than the entire catalog.

pub(crate) mod authentication;
pub(crate) mod binding;
pub mod column;
pub(crate) mod column_property;
pub mod column_snapshot;
pub(crate) mod config;
pub(crate) mod dictionary;
pub(crate) mod flow;
pub(crate) mod flow_edge;
pub(crate) mod flow_node;
pub(crate) mod granted_role;
pub(crate) mod handler;
pub(crate) mod identity;
pub(crate) mod identity_attribute;
pub(crate) mod identity_attribute_value;
pub(crate) mod migration;
pub(crate) mod namespace;
pub(crate) mod object;
pub mod operator_settings;
pub(crate) mod policy;
#[allow(dead_code)]
pub(crate) mod primary_key;
pub(crate) mod procedure;
#[allow(dead_code)]
pub mod queue;
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
