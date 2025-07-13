// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnId;
use crate::column_policy::layout::column_policy;
use crate::column_policy::{ColumnPolicy, ColumnPolicyKind};
use crate::key::{ColumnPolicyKey, EncodableKey};
use crate::sequence::SystemSequence;
use crate::{Catalog, Error};
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};
use reifydb_diagnostic::catalog::column_policy_already_exists;

impl Catalog {
    pub(crate) fn create_column_policy<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        column: ColumnId,
        policy: ColumnPolicyKind,
    ) -> crate::Result<ColumnPolicy> {
        let (policy_kind, _value_kind) = policy.to_u8();
        for existing in Catalog::list_column_policies(tx, column)? {
            let (existing_kind, _) = existing.policy.to_u8();
            if existing_kind == policy_kind {
                let column =
                    Catalog::get_column(tx, column)?.map(|col| col.name).unwrap_or("".to_string());
                return Err(Error(column_policy_already_exists(&policy.to_string(), &column)));
            }
        }

        let id = SystemSequence::next_column_policy_id(tx)?;

        let mut row = column_policy::LAYOUT.allocate_row();
        column_policy::LAYOUT.set_u64(&mut row, column_policy::ID, id);
        column_policy::LAYOUT.set_u64(&mut row, column_policy::COLUMN, column);

        {
            let (policy, value) = policy.to_u8();
            column_policy::LAYOUT.set_u8(&mut row, column_policy::POLICY, policy);
            column_policy::LAYOUT.set_u8(&mut row, column_policy::VALUE, value);
        }

        tx.set(&ColumnPolicyKey { column, policy: id }.encode(), row)?;

        Ok(ColumnPolicy { id, column, policy })
    }
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::column::{ColumnId, ColumnIndex, ColumnToCreate};
    use crate::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};
    use crate::table::TableId;
    use crate::test_utils::{create_test_table_column, ensure_test_table};
    use reifydb_core::DataType;
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_ok() {
        let mut tx = TestTransaction::new();
        ensure_test_table(&mut tx);
        create_test_table_column(&mut tx, "col_1", DataType::Int2, vec![]);

        let policy = ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Error);
        let result = Catalog::create_column_policy(&mut tx, ColumnId(1), policy.clone()).unwrap();
        assert_eq!(result.column, ColumnId(1));
        assert_eq!(result.policy, policy);
    }

    #[test]
    fn test_create_column_policy_duplicate_error() {
        let mut tx = TestTransaction::new();
        ensure_test_table(&mut tx);

        Catalog::create_column(
            &mut tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "schema",
                table: TableId(1),
                table_name: "table",
                column: "col1".to_string(),
                value: DataType::Int2,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
            },
        )
        .unwrap();

        let policy = ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Undefined);
        Catalog::create_column_policy(&mut tx, ColumnId(1), policy.clone()).unwrap();

        let err = Catalog::create_column_policy(&mut tx, ColumnId(1), policy.clone()).unwrap_err();
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "CA_008");
    }
}
