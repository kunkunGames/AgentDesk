CODESIGN_IDENTITY ?= $(or $(AGENTDESK_CODESIGN_IDENTITY),Developer ID Application: Wonchang Oh (A7LJY7HNGA))
ALLOW_ADHOC_SIGN ?= $(or $(AGENTDESK_ALLOW_ADHOC_SIGN),0)
BUNDLE_ID := com.itismyfield.agentdesk
TARGET := target/release/agentdesk

.PHONY: build clean docs-generated docs-generated-check

build:
	cargo build --release
	@identity='$(CODESIGN_IDENTITY)'; \
	allow='$(ALLOW_ADHOC_SIGN)'; \
	if [ -n "$$identity" ] && [ "$$identity" != "-" ] && command -v security >/dev/null 2>&1; then \
		if ! security find-identity -v -p codesigning 2>/dev/null | grep -Fq "$$identity"; then \
			if [ "$$allow" = "1" ]; then \
				echo "⚠ Signing identity not found locally; using explicit ad-hoc build signature override"; \
				identity="-"; \
			else \
				echo "✗ Signing identity not found locally: $$identity"; \
				echo "  Set AGENTDESK_CODESIGN_IDENTITY to a valid Developer ID Application certificate"; \
				echo "  or set AGENTDESK_ALLOW_ADHOC_SIGN=1 for an explicit local override"; \
				exit 1; \
			fi; \
		fi; \
	fi; \
	if [ -z "$$identity" ]; then \
		if [ "$$allow" = "1" ]; then \
			echo "⚠ No signing identity configured; using explicit ad-hoc build signature override"; \
			identity="-"; \
		else \
			echo "✗ No build signing identity configured"; \
			echo "  Set AGENTDESK_CODESIGN_IDENTITY to a valid Developer ID Application certificate"; \
			echo "  or set AGENTDESK_ALLOW_ADHOC_SIGN=1 for an explicit local override"; \
			exit 1; \
		fi; \
	fi; \
	if [ "$$identity" = "-" ] && [ "$$allow" != "1" ]; then \
		echo "✗ Refusing ad-hoc build signing without AGENTDESK_ALLOW_ADHOC_SIGN=1"; \
		exit 1; \
	fi; \
	if [ "$$identity" = "-" ]; then \
		codesign -s "$$identity" --identifier "$(BUNDLE_ID)" --force "$(TARGET)"; \
	else \
		codesign -s "$$identity" --options runtime --identifier "$(BUNDLE_ID)" --force "$(TARGET)"; \
	fi
	@codesign -v "$(TARGET)" && echo "✓ Signed: $(TARGET)"

clean:
	cargo clean

docs-generated:
	python3 scripts/generate_inventory_docs.py

docs-generated-check:
	python3 scripts/generate_inventory_docs.py --check
