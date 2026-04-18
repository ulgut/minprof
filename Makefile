BINARY := minprof
INSTALL_DIR := $(HOME)/.local/bin

.PHONY: build bench test clean install

build:
	cargo build --release

bench:
	cargo bench $(ARGS)

test:
	cargo test $(ARGS)

clean:
	cargo clean

install: build
	install -d $(INSTALL_DIR)
	install -m 755 target/release/$(BINARY) $(INSTALL_DIR)/$(BINARY)
