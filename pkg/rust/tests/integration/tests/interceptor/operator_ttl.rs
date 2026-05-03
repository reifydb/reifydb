// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::{
	core::row::{TtlAnchor, TtlCleanupMode},
	multi_storage::gc::operator::ListOperatorTtls,
};

use super::common::{admin, fresh_db};

#[test]
fn create_view_with_operator_ttl_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create table demo::t { id: uint8 }");
	admin(
		&db,
		"create view demo::v { id: uint8 } as { \
		    from demo::t \
		    distinct { id } with { ttl: { duration: '1m', on: created, mode: drop } } \
		}",
	);

	let ttls = db.engine().catalog().list_operator_ttls();

	assert!(!ttls.is_empty());

	let (_node, ttl) = ttls.into_iter().next().expect("at least one operator TTL");
	assert_eq!(ttl.duration_nanos, 60_000_000_000);
	assert_eq!(ttl.anchor, TtlAnchor::Created);
	assert_eq!(ttl.cleanup_mode, TtlCleanupMode::Drop);
}
