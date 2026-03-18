CC = wllvm -O0 -Xclang -disable-O0-optnone -g -fno-discard-value-names -w
CXX = wllvm++ -O0 -Xclang -disable-O0-optnone -g -fno-discard-value-names -w

CFLAGS = -Wall -Wextra -pthread -I. -DRTC_BENCHMARK -DENDIAN_LITTLE=1 -DENDIAN_BIG=0 -DHAVE_HTONLL=1
LDFLAGS = -pthread

EXTRACT_BC ?= extract-bc
LLVM_DIS ?= llvm-dis
LLVM_NM ?= llvm-nm
OPT ?= opt

TARGET = kv_memcached
IR_TARGET = $(TARGET).ir
IR_BC = $(TARGET).full.bc
IR_LL = $(TARGET).full.ll
IR_OPT_LL = $(TARGET).opt.ll

CORE_SOURCES = assoc.c cache.c globals.c hash.c items.c slabs.c stats.c thread.c util.c
BENCH_SOURCE = kv_memcached.c
ALL_SOURCES = $(BENCH_SOURCE) $(CORE_SOURCES)

GREEN = \033[0;32m
YELLOW = \033[0;33m
NC = \033[0m

all: info $(TARGET)
	@echo "$(GREEN)✓ build complete$(NC)"

info:
	@echo "$(YELLOW)========================================$(NC)"
	@echo "$(YELLOW)  Build memcached RTC benchmark$(NC)"
	@echo "$(YELLOW)========================================$(NC)"

$(TARGET): $(ALL_SOURCES)
	$(CC) $(CFLAGS) -o $(TARGET) $(ALL_SOURCES) $(LDFLAGS)

$(IR_TARGET): $(ALL_SOURCES)
	$(CC) $(CFLAGS) -o $(IR_TARGET) $(ALL_SOURCES) $(LDFLAGS)

ir: info $(IR_TARGET)
	$(EXTRACT_BC) ./$(IR_TARGET)
	mv -f ./$(IR_TARGET).bc ./$(IR_BC)
	$(LLVM_DIS) ./$(IR_BC) -o ./$(IR_LL)
	$(OPT) -S -p=mem2reg,loop-simplify ./$(IR_LL) -o ./$(IR_OPT_LL)
	@echo "$(GREEN)✓ generated $(IR_BC), $(IR_LL), $(IR_OPT_LL)$(NC)"

ir-check: ir
	@$(LLVM_NM) $(IR_BC) | grep -Eq " [Tt] _?memc_set$$"
	@$(LLVM_NM) $(IR_BC) | grep -Eq " [Tt] _?do_store_item$$"
	@$(LLVM_NM) $(IR_BC) | grep -Eq " [Tt] _?assoc_find$$"
	@$(LLVM_NM) $(IR_BC) | grep -Eq " [Tt] _?do_item_alloc$$"
	@echo "$(GREEN)✓ IR contains benchmark and memcached core definitions$(NC)"

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET) $(TARGET).bc $(TARGET).ll $(TARGET).opt.ll
	rm -f $(IR_TARGET) $(IR_TARGET).bc $(IR_BC) $(IR_LL) $(IR_OPT_LL)

.PHONY: all info ir ir-check run clean
