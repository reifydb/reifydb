// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod table {
    use once_cell::sync::Lazy;
    use reifydb_core::Kind;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const SCHEMA: usize = 1;
    pub(crate) const NAME: usize = 2;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Kind::Uint8,  // id
            Kind::Uint8,  // schema id
            Kind::Utf8, // name
        ])
    });
}

pub(crate) mod table_schema {
    use once_cell::sync::Lazy;
    use reifydb_core::Kind;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const NAME: usize = 1;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Kind::Uint8,  // id
            Kind::Utf8, // name
        ])
    });
}
