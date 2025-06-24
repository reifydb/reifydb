// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub(crate) mod column {
    use once_cell::sync::Lazy;
    use reifydb_core::Kind;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const TABLE: usize = 1;
    pub(crate) const NAME: usize = 2;
    pub(crate) const VALUE: usize = 3;
    pub(crate) const INDEX: usize = 4;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Kind::Uint8,  // id
            Kind::Uint8,  // table
            Kind::String, // name
            Kind::Uint1,  // value
            Kind::Uint2,  // index
        ])
    });
}

pub(crate) mod table_column {
    use once_cell::sync::Lazy;
    use reifydb_core::Kind;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const NAME: usize = 1;
    pub(crate) const INDEX: usize = 2;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Kind::Uint8,  // column id
            Kind::String, // column name
            Kind::Uint2,  // column index - position in the table
        ])
    });
}
