// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod database;
mod embedded;
#[cfg(feature = "sub_server")]
mod server;
pub mod traits;

pub use database::DatabaseBuilder;
pub use embedded::EmbeddedBuilder;
#[cfg(feature = "sub_server")]
pub use server::ServerBuilder;
pub use traits::WithSubsystem;
