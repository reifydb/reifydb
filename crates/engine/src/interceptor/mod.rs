// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod catalog_events;
pub(crate) mod materialized_catalog;

pub(crate) use catalog_events::CatalogEventInterceptor;
