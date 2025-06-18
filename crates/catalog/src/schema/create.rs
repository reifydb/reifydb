// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::key::{EncodableKey, SchemaKey};
use crate::schema::Schema;
use crate::schema::layout::schema;
use crate::sequence::SystemSequence;
use crate::{Catalog, Error};
use reifydb_diagnostic::{Diagnostic, Span};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

#[derive(Debug, Clone)]
pub struct SchemaToCreate {
    pub schema_span: Option<Span>,
    pub name: String,
}

impl Catalog {
    pub fn create_schema<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        to_create: SchemaToCreate,
    ) -> crate::Result<Schema> {
        if let Some(schema) = Catalog::get_schema_by_name(tx, &to_create.name)? {
            return Err(Error(Diagnostic::schema_already_exists(
                to_create.schema_span,
                &schema.name,
            )));
        }

        let schema_id = SystemSequence::next_schema_id(tx)?;

        let mut row = schema::LAYOUT.allocate_row();
        schema::LAYOUT.set_u64(&mut row, schema::ID, schema_id);
        schema::LAYOUT.set_str(&mut row, schema::NAME, &to_create.name);

        tx.set(&SchemaKey { schema: schema_id }.encode(), row)?;

        Ok(Catalog::get_schema(tx, schema_id)?.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::schema::create::SchemaToCreate;
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_create_schema() {
        let mut tx = TestTransaction::new();

        let to_create = SchemaToCreate { schema_span: None, name: "test_schema".to_string() };

        // First creation should succeed
        let result = Catalog::create_schema(&mut tx, to_create.clone()).unwrap();
        assert_eq!(result.id, 1);
        assert_eq!(result.name, "test_schema");

        // Creating the same schema again with `if_not_exists = false` should return error
        let err = Catalog::create_schema(&mut tx, to_create).unwrap_err();
        assert_eq!(err.diagnostic().code, "CA_001");
    }
}
