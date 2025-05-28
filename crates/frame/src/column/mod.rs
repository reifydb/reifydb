// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use values::ColumnValues;

mod extend;
mod values;

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data: ColumnValues,
}


impl Column {
    pub fn bool(name: &str, values: impl IntoIterator<Item = bool>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::bool(values) }
    }

    pub fn bool_with_validity(
        name: &str,
        values: impl IntoIterator<Item = bool>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::bool_with_validity(values, validity) }
    }

    pub fn float8(name: &str, values: impl IntoIterator<Item = f64>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::float8(values) }
    }

    pub fn float8_with_validity(
        name: &str,
        values: impl IntoIterator<Item = f64>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::float8_with_validity(values, validity) }
    }

    pub fn int2(name: &str, values: impl IntoIterator<Item = i16>) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int2(values) }
    }

    pub fn int2_with_validity(
        name: &str,
        values: impl IntoIterator<Item = i16>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self { name: name.to_string(), data: ColumnValues::int2_with_validity(values, validity) }
    }

    pub fn text<'a>(name: &str, values: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            name: name.to_string(),
            data: ColumnValues::text(values.into_iter().map(|s| s.to_string())),
        }
    }

    pub fn text_with_validity<'a>(
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
        validity: impl IntoIterator<Item = bool>,
    ) -> Self {
        Self {
            name: name.to_string(),
            data: ColumnValues::text_with_validity(
                values.into_iter().map(|s| s.to_string()),
                validity,
            ),
        }
    }

    pub fn undefined(name: &str, len: usize) -> Self {
        Self { name: name.to_string(), data: ColumnValues::undefined(len) }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
