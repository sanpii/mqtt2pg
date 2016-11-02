CARGO_FLAGS=
TASK=target/debug/mqtt2pg
SRC=Cargo.toml src/main.rs

ifeq ($(APP_ENVIRONMENT),prod)
	TASK=target/release/mqtt2pg
	CARGO_FLAGS+=--release
endif

all: $(TASK)

$(TASK): $(SRC)
	cargo build $(CARGO_FLAGS)

.PHONY: all
