// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![deny(clippy::unwrap_used)]
// #![deny(clippy::expect_used)]

pub use error::Error;

mod error;

pub type PrincipalId = u64;

#[derive(Debug, Clone)]
pub enum Principal {
    Anonymous {},
    System { id: PrincipalId, name: String },
    User { id: PrincipalId, name: String },
}

pub type Result<T> = std::result::Result<T, Error>;
