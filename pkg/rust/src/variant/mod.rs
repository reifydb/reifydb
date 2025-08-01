// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(feature = "embedded")]
pub mod embedded;

#[cfg(feature = "embedded_blocking")]
pub mod embedded_blocking;

#[cfg(feature = "server")]
pub mod server;
