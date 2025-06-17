// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod column_policy {
    use once_cell::sync::Lazy;
    use reifydb_core::ValueKind;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const COLUMN: usize = 1;
    pub(crate) const POLICY: usize = 2;
    pub(crate) const VALUE: usize = 3;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            ValueKind::Uint8, // id
            ValueKind::Uint8, // column
            ValueKind::Uint1, // policy
            ValueKind::Uint1, // value
        ])
    });
}
