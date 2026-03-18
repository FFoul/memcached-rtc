CC := wllvm
CFLAGS := -O0 -Xclang -disable-O0-optnone -g -fno-discard-value-names -w \
	-Wall -Wextra -pthread -I. -Iinclude -DUSE_THREADS -DRTC_BENCHMARK \
	-DENDIAN_LITTLE=1 -DENDIAN_BIG=0 -DHAVE_HTONLL=1 -DHAVE_STDINT_H=1 \
	-DHAVE_STDBOOL_H=1 -DHAVE_UNISTD_H=1
LDFLAGS := -pthread

EXTRACT_BC ?= extract-bc
LLVM_DIS ?= llvm-dis
OPT ?= opt

TARGET := kv_memcached
BC := $(TARGET).full.bc
LL := $(TARGET).full.ll
OPT_LL := $(TARGET).opt.ll

SOURCES := kv_memcached.c assoc.c items.c slabs.c stats.c thread.c

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CC) $(CFLAGS) -o $@ $(SOURCES) $(LDFLAGS)

ir: $(TARGET)
	$(EXTRACT_BC) ./$(TARGET)
	mv -f ./$(TARGET).bc ./$(BC)
	$(LLVM_DIS) ./$(BC) -o ./$(LL)
	$(OPT) -S -p=mem2reg,loop-simplify ./$(LL) -o ./$(OPT_LL)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET) $(BC) $(LL) $(OPT_LL)

.PHONY: all ir run clean
