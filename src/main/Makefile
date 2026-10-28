# Dev helpers for 6.5840 Lab 1. Put this file in src/main/.
#
#   make run-coordinator        terminal 1
#   make run-worker             terminal 2, 3, ...
#   make run-workers N=3        start N workers in one terminal
#   make check                  compare your output with mrsequential
#   make test                   run the official grader
#
# Options: APP=indexer  SOCK=mysock  RACE=   (empty RACE turns -race off)

APP  ?= wc
SOCK ?= sock123
RACE ?= -race
N    ?= 3

.PHONY: build clean run-coordinator run-worker run-workers check test

build:
	go build $(RACE) -buildmode=plugin ../mrapps/$(APP).go

clean:
	rm -f mr-out-* mr-*-* mr-all.txt mr-correct.txt
	rm -rf .seq

run-coordinator: clean build
	go run $(RACE) mrcoordinator.go $(SOCK) pg-*.txt

run-worker:
	@test -f $(APP).so || (echo "$(APP).so missing: run 'make run-coordinator' or 'make build' first" && exit 1)
	go run $(RACE) mrworker.go $(APP).so $(SOCK)

run-workers:
	@test -f $(APP).so || (echo "$(APP).so missing: run 'make run-coordinator' or 'make build' first" && exit 1)
	@for i in $$(seq 1 $(N)); do \
		go run $(RACE) mrworker.go $(APP).so $(SOCK) & \
	done; wait

check:
	@sort mr-out-* | grep . > mr-all.txt
	@rm -rf .seq && mkdir .seq && cd .seq && \
		go run $(RACE) ../mrsequential.go ../$(APP).so ../pg-*.txt && \
		sort mr-out-0 > ../mr-correct.txt
	@if cmp -s mr-all.txt mr-correct.txt; then \
		echo "PASS: output matches mrsequential"; \
	else \
		echo "FAIL: output differs, see: diff mr-all.txt mr-correct.txt"; \
	fi

test:
	bash test-mr.sh