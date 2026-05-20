# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Container/Docker Targets - Build and push container images
# =============================================================================

.PHONY: build-testcontainer push-testcontainer

# Build the test container
build-testcontainer:
	@echo "🐳 Building test container..."
	docker build -f bin/testcontainer/Dockerfile -t reifydb/testcontainer .

# Push the test container to registry
push-testcontainer: check
	@echo "📤 Pushing test container to registry..."
	docker push reifydb/testcontainer