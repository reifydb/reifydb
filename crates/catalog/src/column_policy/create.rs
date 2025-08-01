// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::column::ColumnId;
use crate::column_policy::layout::column_policy;
use crate::column_policy::{ColumnPolicy, ColumnPolicyKind};
use crate::sequence::SystemSequence;
use reifydb_core::interface::{
    ActiveWriteTransaction, ColumnPolicyKey, EncodableKey, UnversionedTransaction,
    VersionedTransaction, VersionedWriteTransaction,
};
use reifydb_core::result::error::diagnostic::catalog::column_policy_already_exists;
use reifydb_core::return_error;

impl Catalog {
    pub(crate) fn create_column_policy<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        column: ColumnId,
        policy: ColumnPolicyKind,
    ) -> crate::Result<ColumnPolicy> {
        let (policy_kind, _value_kind) = policy.to_u8();
        for existing in Catalog::list_column_policies(atx, column)? {
            let (existing_kind, _) = existing.policy.to_u8();
            if existing_kind == policy_kind {
                let column =
                    Catalog::get_column(atx, column)?.map(|col| col.name).unwrap_or("".to_string());
                return_error!(column_policy_already_exists(&policy.to_string(), &column));
            }
        }

        let id = SystemSequence::next_column_policy_id(atx)?;

        let mut row = column_policy::LAYOUT.allocate_row();
        column_policy::LAYOUT.set_u64(&mut row, column_policy::ID, id);
        column_policy::LAYOUT.set_u64(&mut row, column_policy::COLUMN, column);

        {
            let (policy, value) = policy.to_u8();
            column_policy::LAYOUT.set_u8(&mut row, column_policy::POLICY, policy);
            column_policy::LAYOUT.set_u8(&mut row, column_policy::VALUE, value);
        }

        atx.set(&ColumnPolicyKey { column, policy: id }.encode(), row)?;

        Ok(ColumnPolicy { id, column, policy })
    }
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::column::{ColumnId, ColumnIndex, ColumnToCreate};
    use crate::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};
    use crate::test_utils::{create_test_table_column, ensure_test_table};
    use reifydb_core::Type;
    use reifydb_core::interface::TableId;
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_ok() {
        let mut atx = create_test_write_transaction();
        ensure_test_table(&mut atx);
        create_test_table_column(&mut atx, "col_1", Type::Int2, vec![]);

        let policy = ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Error);
        let result = Catalog::create_column_policy(&mut atx, ColumnId(1), policy.clone()).unwrap();
        assert_eq!(result.column, ColumnId(1));
        assert_eq!(result.policy, policy);
    }

    #[test]
    fn test_create_column_policy_duplicate_error() {
        let mut atx = create_test_write_transaction();
        ensure_test_table(&mut atx);

        Catalog::create_column(
            &mut atx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "schema",
                table: TableId(1),
                table_name: "table",
                column: "col1".to_string(),
                value: Type::Int2,
                if_not_exists: false,
                policies: vec![],
                index: ColumnIndex(0),
            },
        )
        .unwrap();

        let policy = ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Undefined);
        Catalog::create_column_policy(&mut atx, ColumnId(1), policy.clone()).unwrap();

        let err = Catalog::create_column_policy(&mut atx, ColumnId(1), policy.clone()).unwrap_err();
        let diagnostic = err.diagnostic();
        assert_eq!(diagnostic.code, "CA_008");
    }
}
