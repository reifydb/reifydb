// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

fn main() -> Result<(), Box<dyn std::error::Error>> {
	tonic_build::configure()
		.protoc_arg("--experimental_allow_proto3_optional")
		.compile_protos(
			&["../../crates/reifydb-network/proto/reifydb.proto"],
			&["../../crates/reifydb-network/proto"],
		)?;
	Ok(())
}
