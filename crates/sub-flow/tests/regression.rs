// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "subscription/common.rs"]
mod common;

#[path = "regression/append_multiplicity.rs"]
mod append_multiplicity;

#[path = "regression/deferred_append_over_transactional_filter.rs"]
mod deferred_append_over_transactional_filter;

#[path = "regression/update_diff_kind.rs"]
mod update_diff_kind;

#[path = "regression/view_dictionary_columns.rs"]
mod view_dictionary_columns;
