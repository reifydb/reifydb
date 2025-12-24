use std::process::Command;

fn main() {
	let output = Command::new("git").args(["rev-parse", "HEAD"]).output().ok();

	if let Some(output) = output.filter(|o| o.status.success()) {
		let git_hash = String::from_utf8_lossy(&output.stdout);
		println!("cargo:rustc-env=GIT_HASH={}", git_hash.trim());
	}

	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-changed=.git/HEAD");
}
