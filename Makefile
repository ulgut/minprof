BINARY := minprof
INSTALL_DIR := $(HOME)/.local/bin

.PHONY: build clean install

build:
	cargo build --release

clean:
	cargo clean

install: build
	install -d $(INSTALL_DIR)
	install -m 755 target/release/$(BINARY) $(INSTALL_DIR)/$(BINARY)
