// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use crate::core::offset::Offset;

/// A random-access sequence of byte rows used for training and encoding.
pub trait Rows {
    /// Number of rows.
    fn num_rows(&self) -> usize;

    /// Total bytes across every row.
    fn total_bytes(&self) -> usize;

    /// Return row `i`.
    fn row(&self, i: usize) -> &[u8];
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ArrowRows<'a, O: Offset> {
    bytes: &'a [u8],
    offsets: &'a [O],
}

impl<'a, O: Offset> ArrowRows<'a, O> {
    #[inline]
    pub(crate) fn new(bytes: &'a [u8], offsets: &'a [O]) -> Self {
        Self { bytes, offsets }
    }
}

impl<O: Offset> Rows for ArrowRows<'_, O> {
    #[inline]
    fn num_rows(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    fn total_bytes(&self) -> usize {
        let first = self.offsets[0].to_usize();
        let last = self.offsets[self.offsets.len() - 1].to_usize();
        last - first
    }

    #[inline]
    fn row(&self, i: usize) -> &[u8] {
        let start = self.offsets[i].to_usize();
        let end = self.offsets[i + 1].to_usize();
        &self.bytes[start..end]
    }
}

impl<T: AsRef<[u8]>> Rows for [T] {
    #[inline]
    fn num_rows(&self) -> usize {
        self.len()
    }

    #[inline]
    fn total_bytes(&self) -> usize {
        self.iter().map(|r| r.as_ref().len()).sum()
    }

    #[inline]
    fn row(&self, i: usize) -> &[u8] {
        self[i].as_ref()
    }
}

impl<R: Rows + ?Sized> Rows for &R {
    #[inline]
    fn num_rows(&self) -> usize {
        (**self).num_rows()
    }

    #[inline]
    fn total_bytes(&self) -> usize {
        (**self).total_bytes()
    }

    #[inline]
    fn row(&self, i: usize) -> &[u8] {
        (**self).row(i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_rows_are_exact_slices() {
        let rows = ArrowRows::new(b"alphabeta", &[0u32, 5, 9]);
        assert_eq!(rows.num_rows(), 2);
        assert_eq!(rows.total_bytes(), 9);
        assert_eq!(rows.row(0), b"alpha");
        assert_eq!(rows.row(1), b"beta");
    }

    #[test]
    fn arrow_rows_support_nonzero_first_offset() {
        let rows = ArrowRows::new(b"_row_", &[1u32, 4]);
        assert_eq!((rows.total_bytes(), rows.row(0)), (3, b"row".as_slice()));
    }

    #[test]
    fn slice_rows_are_exact_slices() {
        let rows: &[&[u8]] = &[b"alpha", b"", b"beta"];
        assert_eq!(rows.num_rows(), 3);
        assert_eq!(rows.total_bytes(), 9);
        assert_eq!(rows.row(0), b"alpha");
        assert_eq!(rows.row(1), b"");
        assert_eq!(rows.row(2), b"beta");
    }

    #[test]
    fn owned_rows_are_supported() {
        let rows = [b"one".to_vec(), b"two".to_vec()];
        assert_eq!(rows.as_slice().row(1), b"two");
    }
}
