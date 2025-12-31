// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod catalog_events;
pub(crate) mod materialized_catalog;

pub(crate) use catalog_events::CatalogEventInterceptor;
