# Release Management Makefile
# This makefile handles versioning and release orchestration for the ReifyDB monorepo

# Default configuration
RELEASE_CONFIG ?= release.toml
SCRIPTS_DIR := scripts
VERSION_FILE := .version

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# Get current version from workspace Cargo.toml
CURRENT_VERSION := $(shell grep -E '^version = ' Cargo.toml | head -n1 | sed 's/version = "\(.*\)"/\1/')

# Helper function to check if scripts exist
define check_script
	@if [ ! -f "$(SCRIPTS_DIR)/$(1)" ]; then \
		echo "$(RED)Error: Required script $(SCRIPTS_DIR)/$(1) not found$(NC)"; \
		exit 1; \
	fi
endef

# Display current version
.PHONY: show-version
show-version:
	@echo "Current version: $(GREEN)$(CURRENT_VERSION)$(NC)"

# Bump major version (x.0.0)
.PHONY: bump-major
bump-major:
	@$(call check_script,update-version.sh)
	@echo "$(YELLOW)Bumping major version from $(CURRENT_VERSION)$(NC)"
	@NEW_VERSION=$$(echo $(CURRENT_VERSION) | awk -F. '{print $$1+1".0.0"}') && \
	echo "$(BLUE)New version: $$NEW_VERSION$(NC)" && \
	$(SCRIPTS_DIR)/update-version.sh $$NEW_VERSION

# Bump minor version (0.x.0)
.PHONY: bump-minor
bump-minor:
	@$(call check_script,update-version.sh)
	@echo "$(YELLOW)Bumping minor version from $(CURRENT_VERSION)$(NC)"
	@NEW_VERSION=$$(echo $(CURRENT_VERSION) | awk -F. '{print $$1"."$$2+1".0"}') && \
	echo "$(BLUE)New version: $$NEW_VERSION$(NC)" && \
	$(SCRIPTS_DIR)/update-version.sh $$NEW_VERSION

# Bump patch version (0.0.x)
.PHONY: bump-patch
bump-patch:
	@$(call check_script,update-version.sh)
	@echo "$(YELLOW)Bumping patch version from $(CURRENT_VERSION)$(NC)"
	@NEW_VERSION=$$(echo $(CURRENT_VERSION) | awk -F. '{print $$1"."$$2"."$$3+1}') && \
	echo "$(BLUE)New version: $$NEW_VERSION$(NC)" && \
	$(SCRIPTS_DIR)/update-version.sh $$NEW_VERSION

# Set specific version
.PHONY: set-version
set-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "$(RED)Error: VERSION not specified. Use: make set-version VERSION=x.y.z$(NC)"; \
		exit 1; \
	fi
	@$(call check_script,update-version.sh)
	@echo "$(YELLOW)Setting version to $(VERSION)$(NC)"
	@$(SCRIPTS_DIR)/update-version.sh $(VERSION)

# Validate release readiness
.PHONY: validate-release
validate-release:
	@$(call check_script,validate-release.sh)
	@echo "$(YELLOW)Validating release readiness...$(NC)"
	@$(SCRIPTS_DIR)/validate-release.sh $(VERSION)

# Check if version already exists
.PHONY: check-version-exists
check-version-exists:
	@$(call check_script,check-version-exists.sh)
	@VERSION="$(VERSION)"; \
	if [ -z "$$VERSION" ]; then \
		VERSION="$(CURRENT_VERSION)"; \
	fi; \
	echo "$(YELLOW)Checking if version $$VERSION already exists...$(NC)" && \
	$(SCRIPTS_DIR)/check-version-exists.sh $$VERSION

# Dry run of release (no actual publishing)
.PHONY: release-dry-run
release-dry-run:
	@echo "$(YELLOW)=====================================$(NC)"
	@echo "$(YELLOW)     DRY RUN - RELEASE PROCESS$(NC)"
	@echo "$(YELLOW)=====================================$(NC)"
	@if [ -z "$(VERSION)" ]; then \
		echo "$(BLUE)No VERSION specified, will auto-increment patch version$(NC)"; \
		NEW_VERSION=$$(echo $(CURRENT_VERSION) | awk -F. '{print $$1"."$$2"."$$3+1}'); \
	else \
		NEW_VERSION=$(VERSION); \
	fi; \
	echo "$(GREEN)Target version: $$NEW_VERSION$(NC)"; \
	echo ""; \
	echo "$(YELLOW)Steps that would be executed:$(NC)"; \
	echo "  1. Check if version $$NEW_VERSION already exists"; \
	echo "  2. Validate release readiness"; \
	echo "  3. Update all package versions to $$NEW_VERSION"; \
	echo "  4. Create git commit and tag v$$NEW_VERSION"; \
	echo "  5. Publish packages:"; \
	echo "     - Rust crates to crates.io"; \
	echo "     - TypeScript packages to npm"; \
	echo "  6. Push commit and tag to remote"; \
	echo ""; \
	echo "$(YELLOW)Current state:$(NC)"; \
	echo "  Current version: $(CURRENT_VERSION)"; \
	echo "  Git branch: $$(git branch --show-current)"; \
	echo "  Git status: $$(git status --porcelain | wc -l) uncommitted changes"; \
	echo ""; \
	echo "$(BLUE)This is a dry run - no changes will be made$(NC)"

# Main release target
.PHONY: release
release:
	@set -e; \
	echo "$(YELLOW)=====================================$(NC)"; \
	echo "$(YELLOW)        RELEASE PROCESS$(NC)"; \
	echo "$(YELLOW)=====================================$(NC)"; \
	CURRENT_BRANCH=$$(git branch --show-current); \
	if [ "$$CURRENT_BRANCH" != "release" ]; then \
		echo "$(RED)Error: Releases can only be created from the 'release' branch$(NC)"; \
		echo "$(RED)Current branch: $$CURRENT_BRANCH$(NC)"; \
		echo "$(YELLOW)Please switch to the release branch: git checkout release$(NC)"; \
		exit 1; \
	fi; \
	echo "$(GREEN)✓ On release branch$(NC)"; \
	echo ""; \
	if [ -z "$(VERSION)" ]; then \
		echo "$(BLUE)No VERSION specified, auto-incrementing patch version$(NC)"; \
		NEW_VERSION=$$(echo $(CURRENT_VERSION) | awk -F. '{print $$1"."$$2"."$$3+1}'); \
	else \
		NEW_VERSION=$(VERSION); \
	fi; \
	echo "$(GREEN)Releasing version: $$NEW_VERSION$(NC)"; \
	echo ""; \
	echo "$(YELLOW)[1/7] Checking if version already exists...$(NC)"; \
	make check-version-exists "VERSION=$$NEW_VERSION" || exit 1; \
	echo "$(GREEN)✓ Version $$NEW_VERSION is available$(NC)"; \
	echo ""; \
	echo "$(YELLOW)[2/7] Running full test suite and validation (make all)...$(NC)"; \
	$(MAKE) all || exit 1; \
	echo "$(GREEN)✓ All tests passed and containers built$(NC)"; \
	echo ""; \
	echo "$(YELLOW)[3/7] Validating release readiness...$(NC)"; \
	make validate-release "VERSION=$$NEW_VERSION" || exit 1; \
	echo "$(GREEN)✓ Validation passed$(NC)"; \
	echo ""; \
	echo "$(YELLOW)[4/7] Updating all package versions...$(NC)"; \
	make set-version "VERSION=$$NEW_VERSION" || exit 1; \
	echo "$(GREEN)✓ Versions updated$(NC)"; \
	echo ""; \
	echo "$(YELLOW)[5/7] Creating git commit and tag...$(NC)"; \
	if [ ! -f "$(SCRIPTS_DIR)/git-release.sh" ]; then echo "$(RED)Error: Required script $(SCRIPTS_DIR)/git-release.sh not found$(NC)"; exit 1; fi; \
	$(SCRIPTS_DIR)/git-release.sh $$NEW_VERSION || exit 1; \
	echo "$(GREEN)✓ Git operations completed$(NC)"; \
	echo ""; \
	echo "$(YELLOW)[6/7] Publishing packages...$(NC)"; \
	if [ ! -f "$(SCRIPTS_DIR)/publish-release.sh" ]; then echo "$(RED)Error: Required script $(SCRIPTS_DIR)/publish-release.sh not found$(NC)"; exit 1; fi; \
	$(SCRIPTS_DIR)/publish-release.sh $$NEW_VERSION || exit 1; \
	echo "$(GREEN)✓ All packages published$(NC)"; \
	echo ""; \
	echo "$(YELLOW)[7/7] Pushing to remote...$(NC)"; \
	git push origin release --follow-tags || exit 1; \
	echo "$(GREEN)✓ Pushed to remote$(NC)"; \
	echo ""; \
	echo "$(GREEN)=====================================$(NC)"; \
	echo "$(GREEN)  RELEASE $$NEW_VERSION COMPLETED!$(NC)"; \
	echo "$(GREEN)=====================================$(NC)"

# Quick release (patch version)
.PHONY: release-patch
release-patch:
	@$(MAKE) release

# Quick release (minor version)
.PHONY: release-minor
release-minor:
	@NEW_VERSION=$$(echo $(CURRENT_VERSION) | awk -F. '{print $$1"."$$2+1".0"}') && \
	$(MAKE) release VERSION=$$NEW_VERSION

# Quick release (major version)
.PHONY: release-major
release-major:
	@NEW_VERSION=$$(echo $(CURRENT_VERSION) | awk -F. '{print $$1+1".0.0"}') && \
	$(MAKE) release VERSION=$$NEW_VERSION

# Rollback a release (best effort)
.PHONY: rollback-release
rollback-release:
	@if [ -z "$(VERSION)" ]; then \
		echo "$(RED)Error: VERSION not specified. Use: make rollback-release VERSION=x.y.z$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)WARNING: This will attempt to rollback version $(VERSION)$(NC)"
	@echo "$(YELLOW)Note: Published packages cannot always be unpublished$(NC)"
	@echo ""
	@read -p "Are you sure? (y/N) " -n 1 -r; \
	echo ""; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "$(YELLOW)Attempting rollback...$(NC)"; \
		git tag -d v$(VERSION) 2>/dev/null || true; \
		git push origin :refs/tags/v$(VERSION) 2>/dev/null || true; \
		echo "$(YELLOW)Git tag removed (if it existed)$(NC)"; \
		echo "$(RED)Note: Published packages on crates.io and npm cannot be automatically removed$(NC)"; \
		echo "$(RED)You may need to publish a new patch version instead$(NC)"; \
	else \
		echo "$(BLUE)Rollback cancelled$(NC)"; \
	fi

# Help target
.PHONY: help-release
help-release:
	@echo "$(YELLOW)ReifyDB Release Management$(NC)"
	@echo ""
	@echo "$(BLUE)Version Commands:$(NC)"
	@echo "  make show-version          - Display current version"
	@echo "  make bump-major            - Increment major version (x.0.0)"
	@echo "  make bump-minor            - Increment minor version (0.x.0)"
	@echo "  make bump-patch            - Increment patch version (0.0.x)"
	@echo "  make set-version VERSION=x.y.z - Set specific version"
	@echo ""
	@echo "$(BLUE)Release Commands:$(NC)"
	@echo "  make release               - Release with auto-increment patch"
	@echo "  make release VERSION=x.y.z - Release specific version"
	@echo "  make release-patch         - Quick patch release"
	@echo "  make release-minor         - Quick minor release"
	@echo "  make release-major         - Quick major release"
	@echo "  make release-dry-run       - Preview release without executing"
	@echo ""
	@echo "$(BLUE)Validation Commands:$(NC)"
	@echo "  make validate-release      - Check release readiness"
	@echo "  make check-version-exists  - Check if version exists"
	@echo ""
	@echo "$(BLUE)Rollback Commands:$(NC)"
	@echo "  make rollback-release VERSION=x.y.z - Attempt to rollback release"
	@echo ""
	@echo "$(YELLOW)Current version: $(CURRENT_VERSION)$(NC)"