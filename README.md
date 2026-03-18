# Memcached Analysis Port

This directory contains an early multithreaded memcached port for Delphi.

- Upstream baseline: early memcached snapshot imported into [`orig/`](./orig)
- Purpose: build a no-`libevent` analysis target that preserves memcached's
  worker-thread and shared-cache behavior
- Transport model: deterministic in-process harness using `socketpair()`
- Event model: local compatibility shim in [`include/event.h`](./include/event.h)
  and [`src/event.c`](./src/event.c)
- Whole-program IR build: `wllvm` + `extract-bc`

Useful commands:

- `make -C applications/memcached`
- `make -C applications/memcached run`
- `make -C applications/memcached ir`
- `make -C applications/memcached ir-check`

The analyzer-facing artifact is copied to [`applications/IR/memcached.opt.ll`](../IR/memcached.opt.ll).
