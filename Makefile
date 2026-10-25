# swimring — every quality gate is reachable from `make check`.

GO        ?= go
GOLANGCI_LINT ?= golangci-lint
COVER_MIN ?= 80
# Core packages counted towards the coverage gate (the CLI is excluded).
COVER_PKGS = ./,./clock,./transport,./sim,./swim,./ring,./cache,./experiment
COVER_OUT ?= coverage.out

.PHONY: check fmt-check fmt vet lint test race cover demo eval bench fuzz build clean

check: fmt-check vet lint test cover ## all gates: format, vet/type-check, lint, tests (race), coverage

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	@echo "fmt-check: ok"

fmt:
	gofmt -w .

vet: ## compile everything (including tests) and run go vet
	$(GO) build ./...
	$(GO) vet ./...

lint:
	$(GOLANGCI_LINT) run ./...

test: ## unit + integration + property tests with the race detector
	$(GO) test -race -count=1 ./...

cover:
	$(GO) test -count=1 -coverpkg=$(COVER_PKGS) -coverprofile=$(COVER_OUT) ./... > /dev/null
	@./scripts/coverage.sh $(COVER_OUT) $(COVER_MIN)

demo: ## offline end-to-end demo (simulated cluster + loopback UDP)
	$(GO) run ./cmd/swimring demo

eval: ## regenerate results/EVAL.md from seeded simulations
	$(GO) run ./cmd/swimring eval -out results/EVAL.md

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./ring ./swim ./cache

fuzz: ## short fuzzing pass over every decoder and the ring
	$(GO) test ./swim  -run '^$$' -fuzz FuzzDecodePacket  -fuzztime 20s
	$(GO) test ./cache -run '^$$' -fuzz FuzzDecodeMessage -fuzztime 20s
	$(GO) test ./ring  -run '^$$' -fuzz FuzzRingOps       -fuzztime 20s
	$(GO) test ./ring  -run '^$$' -fuzz FuzzLookupN       -fuzztime 20s

build:
	$(GO) build -o bin/swimring ./cmd/swimring

clean:
	rm -rf bin $(COVER_OUT)
