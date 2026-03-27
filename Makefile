IDENTITY := Developer ID Application: Wonchang Oh (A7LJY7HNGA)
BUNDLE_ID := com.itismyfield.agentdesk
TARGET := target/release/agentdesk

.PHONY: build clean

build:
	cargo build --release
	codesign -s "$(IDENTITY)" --options runtime --identifier "$(BUNDLE_ID)" --force "$(TARGET)"
	@codesign -v "$(TARGET)" && echo "✓ Signed: $(TARGET)"

clean:
	cargo clean
