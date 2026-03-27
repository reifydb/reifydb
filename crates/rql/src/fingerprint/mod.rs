// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Statement and request fingerprinting for query pattern identification.
//!
//! Fingerprints collapse queries that differ only in literal values into
//! a single identity, enabling aggregated query statistics.

pub mod request;
pub mod statement;
mod walk;

#[cfg(test)]
mod tests;
