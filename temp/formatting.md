# Install nightly toolchain
rustup toolchain install nightly

# Format with nightly (one-time)
cargo +nightly fmt --all

# Or set nightly as default for rustfmt only
rustup component add rustfmt --toolchain nightly
