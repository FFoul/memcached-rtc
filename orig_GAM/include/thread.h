// Copyright (c) 2018 The GAM Authors

#ifndef GAM_THREAD_H_
#define GAM_THREAD_H_

#include <pthread.h>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <string>
#include <vector>
#include "structure.h"

// Forward declarations
class Worker;
class GAlloc;
class Client;
class drf_manager;
class lrf_manager;

// =============================================================================
// Global GAlloc Management - Thread-Safe Access to GAM Resources
// =============================================================================

// Global thread-local GAlloc instance - defined in thread.cc
// This is the primary interface for accessing GAM memory operations
// Each thread automatically gets its own GAlloc instance
extern thread_local GAlloc* g_thread_galloc;

// Main thread's GAlloc instance (for server initialization) - defined in thread.cc
extern GAlloc* main_galloc;

extern thread_local lrf_manager* g_thread_lrf_manager;
extern drf_manager* main_drf_manager;

// Ensure current thread has a GAlloc instance
// This function is thread-safe and can be called multiple times
// Automatically creates a new GAlloc if none exists for current thread
void ensure_thread_galloc(void);

// Constants
#define MAX_REMOTE_THREADS 1024
#define THREAD_MESSAGE_MAGIC 0xDEADBEEF
#define REMOTE_THREAD_ID_BASE 0x1000000000000000ULL

// Thread status enumeration
enum class ThreadStatus {
    CREATED,
    RUNNING, 
    FINISHED,
    ERROR,
    DETACHED
};

// Message types for inter-node thread communication
enum class ThreadMessageType {
    THREAD_CREATE,
    THREAD_CREATE_RESPONSE,
    THREAD_EXECUTE_RESPONSE,
    THREAD_DETACH
};

// Forward declaration
class RemoteThread;

// Thread Manager class for managing remote threads
class ThreadManager {
private:
    static std::atomic<uint64_t> next_thread_id_;
    static std::unordered_map<uint64_t, std::shared_ptr<RemoteThread>> remote_threads_;
    static std::mutex threads_mutex_;

public:
    // Thread ID allocation and management
    static uint64_t allocate_thread_id();
    static void register_remote_thread(std::shared_ptr<RemoteThread> thread);
    static std::shared_ptr<RemoteThread> find_remote_thread(uint64_t thread_id);
    static void cleanup_thread(uint64_t thread_id);
    static bool is_remote_thread_id(uint64_t thread_id);
    
    // Initialization and cleanup
    static void initialize();
    static void cleanup();
};

// Remote thread descriptor
class RemoteThread {
public:
    uint64_t thread_id;
    int target_server_id;
    std::atomic<ThreadStatus> status;
    std::mutex mutex;
    std::condition_variable cv;
    void* return_value;
    std::string function_name;
    void* args;
    
    // Constructor
    RemoteThread(uint64_t id, int server_id, const std::string& func_name);
    
    // Destructor
    ~RemoteThread();
    
    // Thread synchronization methods
    void wait_for_completion();
    void notify_completion(void* result);
    void set_error();
    bool is_finished() const;
};

// Internal remote threading functions (C++ interface)
int remote_pthread_create(pthread_t* thread, const pthread_attr_t* attr,
                         void* (*start_routine)(void*), void* arg, int de_target = -1);

// DSM pthread implementation functions (internal)
int dsm_pthread_create_impl(pthread_t* thread, const pthread_attr_t* attr,
                            void* (*start_routine)(void*), void* arg);
int dsm_pthread_join_impl(pthread_t thread, void** retval);
int dsm_pthread_detach_impl(pthread_t thread);

// Remote thread execution functions
void* remote_thread_executor(void* arg);

// =============================================================================
// DSM Barrier Implementation (internal)
// =============================================================================

// Barrier internal structure - stored in DSM
struct dsm_barrier_internal_t {
    unsigned int count;       // Total number of threads to synchronize
    unsigned int waiting;     // Current number of waiting threads
    unsigned int generation;  // Barrier generation (for reuse)
};

// DSM pthread barrier implementation functions
int dsm_pthread_barrier_init_impl(GAddr barrier_gaddr,
                                   const pthread_barrierattr_t* attr,
                                   unsigned int count);
int dsm_pthread_barrier_wait_impl(GAddr barrier_gaddr);

// =============================================================================
// Thread wrapper support structures (internal implementation)
// =============================================================================

// Thread wrapper data structure for automatic GAlloc management
struct ThreadWrapperData {
    void* (*original_func)(void*);
    void* original_arg;
};

// Thread entry wrapper function declaration
void* thread_entry_wrapper(void* arg);


// Remote thread execution data structure
struct RemoteExecutionData {
    std::string function_name;
    void* args;
    uint64_t thread_id;  // Added for completion notification
};


// Utility functions
int select_random_worker();
bool should_execute_remotely();
std::string resolve_function_name(void* (*start_routine)(void*));

// Remote thread messaging functions
void send_remote_thread_request(int target_server_id, uint64_t thread_id, 
                               const std::string& func_name, void* arg);
void send_remote_thread_completion(uint64_t thread_id, void* result, bool error);
void handle_remote_thread_completion(uint64_t thread_id, void* result, bool error);
void register_remote_thread_mapping(uint64_t thread_id, pthread_t local_thread, Client* source_client);

#endif // GAM_THREAD_H_
