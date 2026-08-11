// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The surface external code extends ReifyDB through. Everything here is a stable contract, versioned on both
//! sides, and the extern-C layer never leaks an engine-internal type - an extension sees only `reifydb-value` and this
//! crate's re-exports, because anything else would tie extension ABI to engine refactors.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod connector;
pub mod dictionary;
pub mod error;
pub mod extern_c;
pub mod marshal;
pub mod operator;
pub mod procedure;
pub mod rql;
pub mod state;
pub mod transform;
