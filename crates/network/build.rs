// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../crates/network/proto/reifydb.proto")?;
    Ok(())
}
