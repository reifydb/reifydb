// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Operator-specific compilation implementations

pub(crate) mod aggregate;
pub(crate) mod apply;
pub(crate) mod distinct;
pub(crate) mod extend;
pub(crate) mod filter;
pub(crate) mod join;
pub(crate) mod map;
pub(crate) mod sort;
pub(crate) mod take;
pub(crate) mod window;
