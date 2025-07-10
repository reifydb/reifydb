// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod schema {
    use once_cell::sync::Lazy;
    use reifydb_core::DataType;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const NAME: usize = 1;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            DataType::Uint8,  // id
            DataType::Utf8, // name
        ])
    });
}
