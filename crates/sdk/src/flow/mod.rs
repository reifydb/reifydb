// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow definition surface: the graph of operators, connectors and inputs the engine compiles into a dataflow.
//! The builder hides the catalog plumbing so an extension author never has to know how flows are persisted.

pub mod builder;
