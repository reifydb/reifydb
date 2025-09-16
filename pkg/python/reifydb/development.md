## 🚀 Development & Deployment Guide

### Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
pip install pytest
```

### Build

To build the `reifydb` package locally:

```bash
maturin build
```

To install the built package into your Python environment:

```bash
pip install .
```

### Run Tests

Execute the test suite using:

```bash
pytest
```

### Deployment

To build the package in optimized release mode:

```bash
maturin build --release --strip
```

To publish the package to [PyPI](https://pypi.org/):
                              
make sure the pypi token is set
```env
MATURIN_PYPI_TOKEN=ABCD
```

```bash
maturin publish
```