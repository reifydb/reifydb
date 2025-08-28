fn main() {
	let mut cc = cc::Build::new();
	cc.include("src/xxh/c/");
	cc.file("src/xxh/c/xxhash.c");
	cc.warnings(false);
	cc.compile("xxhash");
}
