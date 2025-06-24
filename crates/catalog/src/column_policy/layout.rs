// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod column_policy {
    use once_cell::sync::Lazy;
    use reifydb_core::Kind;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const COLUMN: usize = 1;
    pub(crate) const POLICY: usize = 2;
    pub(crate) const VALUE: usize = 3;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Kind::Uint8, // id
            Kind::Uint8, // column
            Kind::Uint1, // policy
            Kind::Uint1, // value
        ])
    });
}
