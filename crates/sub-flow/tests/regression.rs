// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "regression/append_multiplicity.rs"]
mod append_multiplicity;

#[path = "regression/apply_partition_immutability.rs"]
mod apply_partition_immutability;

#[path = "regression/deferred_append_over_transactional_filter.rs"]
mod deferred_append_over_transactional_filter;

#[path = "regression/view_dictionary_columns.rs"]
mod view_dictionary_columns;

#[path = "regression/view_read_after_upstream_write.rs"]
mod view_read_after_upstream_write;
