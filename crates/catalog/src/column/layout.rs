// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod column {
    use once_cell::sync::Lazy;
    use reifydb_core::Type;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const TABLE: usize = 1;
    pub(crate) const NAME: usize = 2;
    pub(crate) const VALUE: usize = 3;
    pub(crate) const INDEX: usize = 4;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Type::Uint8, // id
            Type::Uint8, // table
            Type::Utf8,  // name
            Type::Uint1, // value
            Type::Uint2, // index
        ])
    });
}

pub(crate) mod table_column {
    use once_cell::sync::Lazy;
    use reifydb_core::Type;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const NAME: usize = 1;
    pub(crate) const INDEX: usize = 2;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            Type::Uint8, // column id
            Type::Utf8,  // column name
            Type::Uint2, // column index - position in the table
        ])
    });
}
