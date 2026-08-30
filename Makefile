# Manooch

GO         ?= go
BIN        ?= bin
CONFIG     ?= ./config
EXCHANGE   ?= BINANCE
COMPOSE    ?= docker compose -f deploy/docker-compose.yml
LOCALCONF  := .local/config

.PHONY: all proto build test test-integration lint run validate up down clean

all: build

## proto: regenerate gen/ from schema/manooch.proto. Commit the result.
proto:
	protoc --proto_path=schema \
	       --go_out=. --go_opt=module=github.com/you/manooch \
	       schema/manooch.proto
	$(GO) build ./gen/...

## build: build all three binaries into $(BIN)/
build:
	$(GO) build -trimpath -o $(BIN)/ ./cmd/...

## test: unit tests
test:
	$(GO) test ./...

## test-integration: tests that need Docker and a real Redis
test-integration:
	$(GO) test -tags=integration -count=1 -timeout=10m ./...

## lint: vet plus a gofmt check
lint:
	$(GO) vet ./...
	@out=$$(gofmt -l $$($(GO) list -f '{{.Dir}}' ./...)); \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

## validate: load and print the resolved config without opening anything
validate:
	$(GO) run ./cmd/manooch-feed --exchange=$(EXCHANGE) --config=$(CONFIG) --validate

## run: run the feed against a Redis on localhost, publishing synthetic data.
## The committed config points at the compose service name, so this makes a
## local copy that points at 127.0.0.1 instead.
run: $(LOCALCONF)
	$(GO) run ./cmd/manooch-feed --exchange=$(EXCHANGE) --config=$(LOCALCONF) --synthetic

$(LOCALCONF): $(wildcard config/*.yaml) $(wildcard config/venues/*.yaml)
	@mkdir -p $(dir $(LOCALCONF))
	@rm -rf $(LOCALCONF)
	@cp -r config $(LOCALCONF)
	@sed -i.bak 's|addr: "redis:6379"|addr: "127.0.0.1:6379"|' $(LOCALCONF)/defaults.yaml && rm -f $(LOCALCONF)/defaults.yaml.bak
	@echo "wrote $(LOCALCONF) (redis on 127.0.0.1)"

## up: bring up Redis and the feed
up:
	$(COMPOSE) up --build -d
	$(COMPOSE) ps

## down: tear it all down
down:
	$(COMPOSE) down -v

## clean: remove build output and local overrides
clean:
	rm -rf $(BIN) .local
