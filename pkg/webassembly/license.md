# ReifyDB License Information

## Overall License

ReifyDB is licensed under a dual licensing model:
- **AGPL-3.0** for open-source users and contributors  
- **Commercial license** available for proprietary use cases

## Project Components License Breakdown

The following table shows the license for each component in the ReifyDB repository:

### Core Components (AGPL-3.0)

| Component | Path | License | Description |
|-----------|------|---------|-------------|
| **Core Crates** | | | |
| auth | `crates/auth` | AGPL-3.0 | Authentication and authorization |
| catalog | `crates/catalog` | AGPL-3.0 | Database catalog management |
| cdc | `crates/cdc` | AGPL-3.0 | Change data capture |
| core | `crates/core` | AGPL-3.0 | Core database functionality |
| engine | `crates/engine` | AGPL-3.0 | Query execution engine |
| hash | `crates/hash` | AGPL-3.0 | Hashing utilities |
| network | `crates/network` | AGPL-3.0 | Network layer |
| rql | `crates/rql` | AGPL-3.0 | ReifyDB Query Language |
| storage | `crates/storage` | AGPL-3.0 | Storage engine |
| sub-admin | `crates/sub-admin` | AGPL-3.0 | Admin subsystem |
| sub-api | `crates/sub-api` | AGPL-3.0 | API subsystem |
| sub-flow | `crates/sub-flow` | AGPL-3.0 | Flow control subsystem |
| sub-logging | `crates/sub-logging` | AGPL-3.0 | Logging subsystem |
| sub-server | `crates/sub-server` | AGPL-3.0 | Server subsystem |
| testing | `crates/testing` | AGPL-3.0 | Testing utilities |
| transaction | `crates/transaction` | AGPL-3.0 | Transaction management |
| **Binaries** | | | |
| cli | `bin/cli` | AGPL-3.0 | Command-line interface |
| server | `bin/server` | AGPL-3.0 | ReifyDB server daemon |
| playground | `bin/playground` | AGPL-3.0 | Interactive playground |
| testcontainer | `bin/testcontainer` | AGPL-3.0 | Test container utilities |

### Client Libraries (MIT)

| Component | Path | License | Description |
|-----------|------|---------|-------------|
| **Type Definitions** | | | |
| reifydb-type | `crates/type` | MIT | Shared type definitions |
| **Client SDKs** | | | |
| Rust Client | `pkg/rust/reifydb-client` | MIT | Rust client library |
| TypeScript Client | `pkg/typescript` | MIT | TypeScript/JavaScript client |
| Python Client | `pkg/python/reifydb` | MIT | Python client library |

### Examples and Tests (AGPL-3.0)

| Component | Path | License | Description |
|-----------|------|---------|-------------|
| Rust Examples | `pkg/rust/examples` | AGPL-3.0 | Example code and tutorials |
| Rust Tests | `pkg/rust/tests` | AGPL-3.0 | Integration tests |

## License Summary

- **Server-side components** (everything in `crates/` except `type`, and `bin/`): **AGPL-3.0**
  - Any modifications or derivative works must be open-sourced under AGPL-3.0
  - Cannot be used in proprietary hosted services without a commercial license

- **Client-side components** (all client SDKs and `crates/type`): **MIT**
  - Can be freely used in proprietary applications
  - No obligation to open-source your application code
  - The `reifydb-type` crate is MIT licensed to allow client libraries to use shared types

## Commercial License

For use cases that require:
- Embedding ReifyDB in proprietary software
- Offering ReifyDB as part of a hosted service
- Keeping modifications private

A commercial license is available. Contact: [founder@reifydb.com](mailto:founder@reifydb.com)

## Full License Text

- [AGPL-3.0 License](https://www.gnu.org/licenses/agpl-3.0.en.html)
- [MIT License](https://opensource.org/licenses/MIT)