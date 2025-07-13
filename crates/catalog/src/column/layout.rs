// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod column {
    use once_cell::sync::Lazy;
    use reifydb_core::DataType;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const TABLE: usize = 1;
    pub(crate) const NAME: usize = 2;
    pub(crate) const VALUE: usize = 3;
    pub(crate) const INDEX: usize = 4;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            DataType::Uint8,  // id
            DataType::Uint8,  // table
            DataType::Utf8, // name
            DataType::Uint1,  // value
            DataType::Uint2,  // index
        ])
    });
}

pub(crate) mod table_column {
    use once_cell::sync::Lazy;
    use reifydb_core::DataType;
    use reifydb_core::row::Layout;

    pub(crate) const ID: usize = 0;
    pub(crate) const NAME: usize = 1;
    pub(crate) const INDEX: usize = 2;

    pub(crate) static LAYOUT: Lazy<Layout> = Lazy::new(|| {
        Layout::new(&[
            DataType::Uint8,  // column id
            DataType::Utf8, // column name
            DataType::Uint2,  // column index - position in the table
        ])
    });
}
