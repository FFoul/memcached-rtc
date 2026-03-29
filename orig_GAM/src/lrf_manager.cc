#include "lrf_manager.h"

// Minimal stub implementation to satisfy linking for thread management.
// Full functionality exists in the race_protocol tree; here we provide
// safe no-op implementations so motivation binaries can link.

lrf_manager::lrf_manager(Worker* worker)
    : wh(nullptr) {
    (void)worker;
}

lrf_manager::~lrf_manager() {
    if (wh) {
        delete wh;
        wh = nullptr;
    }
}

Worker* lrf_manager::getWorker() {
    return nullptr;
}

bool lrf_manager::LocalCas(GAddr, uint64_t, uint64_t) {
    return false;
}

GAddr lrf_manager::MutexMalloc(){
    return (GAddr)0;
}

void lrf_manager::MutexFree(GAddr) {
}

int lrf_manager::MutexLock(GAddr, vector<GAddr>, vector<size_t>) {
    return 0;
}

int lrf_manager::MutexUnlock(GAddr) {
    return 0;
}

int lrf_manager::Read(GAddr, void*, size_t) {
    return -1;
}

int lrf_manager::Write(GAddr, const void*, size_t, bool) {
    return -1;
}

void* lrf_manager::GetWriteCache(GAddr, size_t) { return nullptr; }
void* lrf_manager::GetReadCache(GAddr, size_t) { return nullptr; }
