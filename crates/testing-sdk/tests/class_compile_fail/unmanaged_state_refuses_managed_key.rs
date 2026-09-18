use reifydb_core::key::operator::state::ManagedKey;
use reifydb_sdk::{
	error::Result,
	flow::operator::context::{ClassState, GuestContext, Unmanaged},
};

fn read(ctx: &mut impl GuestContext<Unmanaged>, key: &ManagedKey) -> Result<Option<i64>> {
	ctx.state().get::<i64>(key)
}

fn main() {}
