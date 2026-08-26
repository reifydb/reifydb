use reifydb_store_multi::tier::point::{MultiPointConfig, MultiPointTier};

#[test]
fn print_overhead() {
	let tier = MultiPointTier::new(MultiPointConfig::default()).expect("a default tier must construct");
	println!("point entry_overhead = {}", tier.debug_overhead());
}
