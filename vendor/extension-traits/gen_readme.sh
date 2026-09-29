#!/bin/bash

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
set -x

sed -E \
    -e 's/ext-trait\.rs/PLACEHOLDER/g' \
    -e 's/ext([-_])trait/extension\1traits/g' \
    -e 's/PLACEHOLDER/ext-trait.rs/g' \
    README.md \
    > extension-traits/README.md \
;
