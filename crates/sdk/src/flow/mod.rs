// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Flow definition surface. SDK consumers describe a streaming computation here as a graph of operators,
//! connectors, and inputs; the resulting structure is what the engine compiles into the dataflow that `sub-flow`
//! runs. The builder hides the catalog plumbing so an extension author does not have to know how flows are
//! persisted.

pub mod builder;
