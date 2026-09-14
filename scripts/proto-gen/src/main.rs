// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{env, path::Path};

fn main() {
	let args: Vec<String> = env::args().collect();
	let [_, proto, out_dir] = args.as_slice() else {
		panic!("usage: proto-gen <proto> <out_dir>");
	};
	let proto = Path::new(proto);
	let include = proto.parent().expect("proto path has no parent directory");
	tonic_prost_build::configure()
		.out_dir(out_dir)
		.compile_protos(&[proto], &[include])
		.expect("failed to compile proto");
}
