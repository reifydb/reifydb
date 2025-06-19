// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;
use once_cell::sync::Lazy;
use reifydb_core::row::Layout;
use reifydb_core::{EncodedKey, ValueKind};
use reifydb_diagnostic::Diagnostic;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

static LAYOUT: Lazy<Layout> = Lazy::new(|| Layout::new(&[ValueKind::Uint8]));

pub(crate) struct SequenceGeneratorU64 {}

impl SequenceGeneratorU64 {
    pub(crate) fn next<VS, US>(tx: &mut impl Tx<VS, US>, key: &EncodedKey) -> crate::Result<u64>
    where
        VS: VersionedStorage,
        US: UnversionedStorage,
    {
        let mut bypass = tx.bypass();
        match bypass.get(key)? {
            Some(unversioned) => {
                let mut row = unversioned.row;
                let value = LAYOUT.get_u64(&row, 0);
                let next_value = value.saturating_add(1);

                if value == next_value {
                    return Err(Error(Diagnostic::sequence_exhausted(ValueKind::Uint8)));
                }

                LAYOUT.set_u64(&mut row, 0, next_value);
                bypass.set(key, row)?;
                Ok(value)
            }
            None => {
                let mut new_row = LAYOUT.allocate_row();
                LAYOUT.set_u64(&mut new_row, 0, 2u64);
                bypass.set(key, new_row)?;
                Ok(1)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sequence::u64::{LAYOUT, SequenceGeneratorU64};
    use reifydb_core::{EncodedKey, ValueKind};
    use reifydb_diagnostic::Diagnostic;
    use reifydb_storage::{Unversioned, UnversionedScan, UnversionedSet};
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_ok() {
        let mut tx = TestTransaction::new();
        for expected in 1..1000 {
            let got = SequenceGeneratorU64::next(&mut tx, &EncodedKey::new("sequence")).unwrap();
            assert_eq!(got, expected);
        }

        let unversioned = tx.unversioned();
        let mut unversioned: Vec<Unversioned> = unversioned.scan_unversioned().collect();
        assert_eq!(unversioned.len(), 1);

        let unversioned = unversioned.pop().unwrap();
        assert_eq!(unversioned.key, EncodedKey::new("sequence"));
        assert_eq!(LAYOUT.get_u64(&unversioned.row, 0), 1000);
    }

    #[test]
    fn test_exhaustion() {
        let mut tx = TestTransaction::new();

        let mut row = LAYOUT.allocate_row();
        LAYOUT.set_u64(&mut row, 0, u64::MAX);

        let mut unversioned = tx.unversioned();
        unversioned.set_unversioned(&EncodedKey::new("sequence"), row);

        let err = SequenceGeneratorU64::next(&mut tx, &EncodedKey::new("sequence")).unwrap_err();
        assert_eq!(err.diagnostic(), Diagnostic::sequence_exhausted(ValueKind::Uint8));
    }
}
