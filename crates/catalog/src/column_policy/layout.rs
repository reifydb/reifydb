// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod column_policy {
    use once_cell::sync::Lazy;
    use reifydb_core::Type;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const COLUMN: usize = 1;
    pub(crate) const POLICY: usize = 2;
    pub(crate) const VALUE: usize = 3;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Type::Uint8, // id
            Type::Uint8, // column
            Type::Uint1, // policy
            Type::Uint1, // value
        ])
    });
}
