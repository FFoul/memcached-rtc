#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>

// Get current time in milliseconds
static uint64_t mstime(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

void print_peak_rss() {
    FILE* f = fopen("/proc/self/status", "r");
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmHWM:", 6) == 0) {
            printf("Peak memory: %s\n", line);
        }
    }
    fclose(f);
}


// Configuration constants
#define NUM_SERVERS 32
#define READ_RATIO 50
#define NUM_OPERATIONS 10000000

#define BUCKET_NUM 16777216
#define TAG_BITS 11
#define BKT_BITS 24
#define BKT_MASK ((1 << BKT_BITS) - 1)
#define UNIT_BUCKET_NUM ((16777216 - 1) / NUM_SERVERS + 1)
#define VALUE_SIZE 32


typedef struct {
    size_t key;
    char value[VALUE_SIZE];
} GlobalEntry;

// KV Store structure with per-bucket locking
typedef struct {
    GlobalEntry *entries;
    pthread_mutex_t *bucket_mutexes;
} KVStore;

// Hash function equivalent to Rust bucket function
size_t bucket(size_t key) {
    size_t new_key = (key >> TAG_BITS);
    return new_key & BKT_MASK;
}

// Initialize KV Store
KVStore* kvstore_new() {
    KVStore *store = malloc(sizeof(KVStore));
    
    // Allocate entries
    store->entries = malloc(sizeof(GlobalEntry) * BUCKET_NUM);
    // printf("debug: store->entries address=%llx\n", (unsigned long long)store->entries);

    store->bucket_mutexes = malloc(sizeof(pthread_mutex_t) * BUCKET_NUM);
    // printf("debug: store->bucket_mutexes address=%llx\n", (unsigned long long)store->bucket_mutexes);
    
    // Initialize all entries and bucket mutexes
    for (size_t i = 0; i < BUCKET_NUM; i++) {
        store->entries[i].key = 0;
        memset(store->entries[i].value, 0, VALUE_SIZE);
        pthread_mutex_init(&store->bucket_mutexes[i], NULL);
    }
    
    return store;
}

// Get operation - returns a copy of the value
void kv_get(KVStore *store, size_t key) {
    size_t bucket_id = bucket(key);
    GlobalEntry *entry = &store->entries[bucket_id];
    // printf("debug: &store->entries[bucket_id]=%llx\n", (unsigned long long)&store->entries[bucket_id]);
    // printf("debug: &store->bucket_mutexes[bucket_id]=%llx\n", (unsigned long long)&store->bucket_mutexes[bucket_id]);

    char value[VALUE_SIZE];
    pthread_mutex_lock(&store->bucket_mutexes[bucket_id]);
    // Deep copy the value
    // printf("debug: entry->value address=%llx\n", (unsigned long long)entry->value);
    memcpy(value, entry->value, VALUE_SIZE);
    pthread_mutex_unlock(&store->bucket_mutexes[bucket_id]);
}

// Put operation - deep copy the value
void kv_put(KVStore *store, size_t key, char* value) {
    size_t bucket_id = bucket(key);
    GlobalEntry *entry = &store->entries[bucket_id];
    // printf("debug: &store->entries[bucket_id]=%llx\n", (unsigned long long)&store->entries[bucket_id]);
    // printf("debug: &store->bucket_mutexes[bucket_id]=%llx\n", (unsigned long long)&store->bucket_mutexes[bucket_id]);
    pthread_mutex_lock(&store->bucket_mutexes[bucket_id]);
    // printf("debug: entry->value address=%llx\n", (unsigned long long)entry->value);
    // printf("debug: entry->key=%llu\n", (unsigned long long)key);
    entry->key = key;
    // Deep copy the value instead of shallow copy
    memcpy(entry->value, value, VALUE_SIZE);
    pthread_mutex_unlock(&store->bucket_mutexes[bucket_id]);
}


typedef struct {
    unsigned thread_id;
    KVStore *store;
} Args;


void populate(KVStore *store) {

    char file_name[128];
    snprintf(file_name, sizeof(file_name), "/home/hjx/data/kv/load_%d.csv", NUM_OPERATIONS);

    char *value = malloc(VALUE_SIZE);
    for (int i = 0; i < VALUE_SIZE; i++)
        value[i] = 'x';
    value[VALUE_SIZE - 1] = 0;



    FILE *populate_fp = fopen(file_name, "r");
    if (!populate_fp) {
        printf("fopen populate file");
        exit(1);
    }


    char line[128];
    uint64_t start = mstime();

    int cnt = 0;
    while(cnt++ < NUM_OPERATIONS && fgets(line, sizeof(line), populate_fp)) {
        // Directly convert string to size_t
        size_t key = strtoull(line, NULL, 10);
        size_t bucket_id = bucket(key);
        GlobalEntry *entry = &store->entries[bucket_id];

        entry->key = key;
        // Deep copy the value instead of shallow copy
        memcpy(entry->value, value, VALUE_SIZE);

    }
        
    double duration = mstime() - start;
    printf("populate time: %.2f ms\n", duration);
    
    fclose(populate_fp);
    // free(value);
}

void* benchmark(void *arg) {
    Args *args = (Args*)arg;
    KVStore *store = args->store;
    int thread_id = args->thread_id;


    char *value = malloc(VALUE_SIZE);
    for (int i = 0; i < VALUE_SIZE; i++)
        value[i] = 'x';
    value[VALUE_SIZE - 1] = 0;

    char benchmark_fname[128];
    snprintf(benchmark_fname, sizeof(benchmark_fname), "/home/hjx/data/kv/run_%d_%d_%d.csv", 
                NUM_OPERATIONS, READ_RATIO, thread_id);


    FILE *benchmark_fp = fopen(benchmark_fname, "r");
    if (!benchmark_fp) {
        printf("fopen benchmark file");
        exit(1);
    }

    
    uint64_t start = mstime();
    char line[128]; // KEY_SIZE + operation string
    int cnt = 0;
    
    while(cnt++ < NUM_OPERATIONS && fgets(line, sizeof(line), benchmark_fp)) {
        // Remove newline
        // line[strcspn(line, "\n")] = 0;
        
        // Parse line: "OPERATION,key"
        char *comma = strchr(line, ',');
        *comma = '\0';
        char *operation = line;
        char *key_str = comma + 1;
        
        // Directly convert key string to size_t
        size_t key = strtoull(key_str, NULL, 10);
        
        if (strcmp(operation, "READ") == 0) {
            kv_get(store, key);
        } else if (strcmp(operation, "UPDATE") == 0) {
            kv_put(store, key, value);
        }
    }
    
    double duration = mstime() - start;
    printf("Thread %d: benchmark time: %.2f ms\n", args->thread_id, duration);
    printf("Thread %d: operation num: %d, throughput: %.2f ops/s\n", 
           args->thread_id, cnt - 1, (cnt - 1) * 1000.0 / duration);
        
    fclose(benchmark_fp);
    // free(value);

    return NULL;
}

// Main benchmark function
void zipf_bench(KVStore *store) {
    pthread_t threads[NUM_SERVERS];
    Args args[NUM_SERVERS];
    // Populate phase
    printf("\n=== Population Phase ===\n");
    
    populate(store);


    // Benchmark phase
    printf("\n=== Benchmark Phase ===\n");
    uint64_t start_bench = mstime();
    for(int i = 0; i < NUM_SERVERS; i++) {
        args[i].thread_id = i;
        args[i].store = store;
        pthread_create(&threads[i], NULL, benchmark, (void*)&args[i]);
    }
    for(int i = 0; i < NUM_SERVERS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    uint64_t end_bench = mstime();
    printf("Benchmark phase complete, duration: %.2f ms\n", (double)(end_bench - start_bench));
}

// Cleanup function
void kvstore_destroy(KVStore *store) {
    if (store) {
        if (store->bucket_mutexes) {
            for (size_t i = 0; i < BUCKET_NUM; i++) {
                pthread_mutex_destroy(&store->bucket_mutexes[i]);
            }
            // free(store->bucket_mutexes);
        }
        if (store->entries) {
            // No need to free individual values anymore (they're inline arrays)
            // free(store->entries);
        }
        // free(store);
    }
}

int main() {
    // Initialize KV store
    KVStore *store = kvstore_new();

    
    // Run benchmark
    zipf_bench(store);
    
    printf("\nCleaning up...\n");
    kvstore_destroy(store);
    printf("Done!\n");

        print_peak_rss();
    
    return 0;
}