CC := wllvm
EXTRACT_BC ?= extract-bc
LLVM_DIS ?= llvm-dis
OPT ?=opt

BIN := memcached_rtc
FULL_BC := $(BIN).full.bc
FULL_LL := $(BIN).full.ll
OPT_LL := $(BIN).opt.ll

SOURCES := \
	assoc.c \
	cache.c \
	globals.c \
	hash.c \
	items.c \
	rtc_driver.c \
	rtc_runtime.c \
	slabs.c \
	stats.c \
	thread.c \
	util.c

CPPFLAGS := -DRTC_BENCHMARK -DVERSION=\"1.4.10-rtc\" -DHAVE_HTONLL=1 -DENDIAN_LITTLE=1 -DENDIAN_BIG=0
CFLAGS := -O0 -Xclang -disable-O0-optnone -g -fno-discard-value-names -std=gnu11 -pthread
LDFLAGS := -pthread

.PHONY: all clean run ir ir-check

all: $(BIN)

$(BIN): $(SOURCES)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(SOURCES) -o $@ $(LDFLAGS)

run: $(BIN)
	./$(BIN)

ir: $(FULL_BC) $(FULL_LL) $(OPT_LL)

$(FULL_BC): $(BIN)
	rm -f $(BIN).bc $(FULL_BC)
	$(EXTRACT_BC) ./$(BIN)
	mv -f $(BIN).bc $(FULL_BC)

$(FULL_LL): $(FULL_BC)
	$(LLVM_DIS) $(FULL_BC) -o $(FULL_LL)

$(OPT_LL): $(FULL_BC)
	$(OPT) -S -passes='mem2reg,simplifycfg' $(FULL_BC) -o $(OPT_LL)

ir-check: $(FULL_LL)
	grep -q "define .*@assoc_init" $(FULL_LL)
	grep -q "define .*@do_item_alloc" $(FULL_LL)
	grep -q "define .*@store_item" $(FULL_LL)
	grep -q "define .*@add_delta" $(FULL_LL)
	grep -q "define .*@do_store_item" $(FULL_LL)
	grep -q "define .*@do_add_delta" $(FULL_LL)

clean:
	rm -f $(BIN) $(FULL_BC) $(FULL_LL) $(OPT_LL) $(BIN).bc
