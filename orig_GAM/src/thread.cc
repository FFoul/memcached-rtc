// Copyright (c) 2018 The GAM Authors

#include "thread.h"
#include "gallocator.h"
#include "drf_manager.h"
#include "lrf_manager.h"
#include "worker.h"
#include "log.h"
#include <iostream>
#include <random>
#include <cstring>
#include <sched.h>
#include <errno.h>

// =============================================================================
// Global GAlloc Management - Thread-Safe Access to GAM Resources
// =============================================================================

// Thread local storage for GAlloc management using TLS
thread_local GAlloc* g_thread_galloc = nullptr;
GAlloc* main_galloc = nullptr;

drf_manager* main_drf_manager = nullptr;
thread_local lrf_manager* g_thread_lrf_manager = nullptr;

// Remote thread mapping management
// Maps remote thread_id to source client for completion notification
struct RemoteThreadMapping {
    Client* source_client;
    uint64_t thread_id;  // For debugging/verification
};

static std::unordered_map<uint64_t, RemoteThreadMapping> remote_thread_mappings;
static std::mutex remote_thread_mappings_mutex;

// Ensure current thread has a GAlloc instance
void ensure_thread_galloc() {
    if (g_thread_galloc == nullptr) {  
        Worker* worker = GAllocFactory::GetWorker();
        if(worker == nullptr) {
            printf("Error: Worker is not initialized\n");
            return;
        }
        g_thread_galloc = new GAlloc(worker);
        // printf("Thread %ld: Auto-created GAlloc instance\n", pthread_self());
    }
}

void ensure_thread_lrf_manager() {
    if (g_thread_lrf_manager == nullptr) {  
        Worker* worker = GAllocFactory::GetWorker();
        if(worker == nullptr) {
            printf("Error: Worker is not initialized\n");
            return;
        }
        g_thread_lrf_manager = new lrf_manager(worker);
        // printf("Thread %ld: Auto-created lrf_manager instance\n", pthread_self());
    }
}

// Random number generator for server selection
static std::random_device rd;
static std::mt19937 gen(rd());

// =============================================================================
// Thread wrapper and GAlloc management
// =============================================================================


// Thread entry wrapper for local threads - ensures each thread has its own GAlloc
void* thread_entry_wrapper(void* arg) {
    ThreadWrapperData* wrapper = static_cast<ThreadWrapperData*>(arg);
    
    // Auto-initialize GAlloc for this thread
    ensure_thread_galloc();
    ensure_thread_lrf_manager();
    
    // Store original parameters
    void* (*original_func)(void*) = wrapper->original_func;
    void* original_arg = wrapper->original_arg;
    
    // Clean up wrapper data
    delete wrapper;
    
    // Call original function
    void* result = original_func(original_arg);
    
    // Cleanup GAlloc for this thread
    if (g_thread_galloc != nullptr) {
        delete g_thread_galloc;
        g_thread_galloc = nullptr;
        // printf("Thread %ld: Auto-cleaned GAlloc instance\n", pthread_self());
    }
    if( g_thread_lrf_manager != nullptr) {
        delete g_thread_lrf_manager;
        g_thread_lrf_manager = nullptr;
        // printf("Thread %ld: Auto-cleaned lrf_manager instance\n", pthread_self());
    }
    
    return result;
}


// Remote thread execution wrapper with proper threading
void* remote_thread_executor(void* arg) {
    RemoteExecutionData* data = (RemoteExecutionData*)arg;
    std::string function_name = data->function_name;
    void* args = data->args;
    uint64_t thread_id = data->thread_id;

    ensure_thread_galloc();
    ensure_thread_lrf_manager();
    
    printf("Remote executor: Thread %ld executing function %s (remote_thread_id=%lu)\n", 
           pthread_self(), function_name.c_str(), thread_id);
    
    void* result = nullptr;
    bool error = false;
    
    try {
        // Look up and execute the function
        extern std::string lookup_function_name(void* func_ptr);
        extern void* lookup_function_pointer(const std::string& func_name);
        
        void* (*func)(void*) = (void*(*)(void*))lookup_function_pointer(function_name);
        if (!func) {
            printf("Error: Function %s not found in registry\n", function_name.c_str());
            error = true;
            result = nullptr;
        } else {
            result = func(args);
            // printf("Remote executor: Function %s completed successfully with result %p\n", 
            //        function_name.c_str(), result);
        }
        
    } catch (const std::exception& e) {
        printf("Error in remote execution of %s: %s\n", function_name.c_str(), e.what());
        error = true;
        result = nullptr;
    }
    
    // Send completion notification back to calling server
    send_remote_thread_completion(thread_id, result, error);
    
    // Clean up GAlloc for this thread (like thread_entry_wrapper does)
    if (g_thread_galloc != nullptr) {
        delete g_thread_galloc;
        g_thread_galloc = nullptr;
        // printf("Remote executor: Thread %ld cleaned up GAlloc\n", pthread_self());
    }
    if (g_thread_lrf_manager != nullptr) {
        delete g_thread_lrf_manager;
        g_thread_lrf_manager = nullptr;
        // printf("Remote executor: Thread %ld cleaned up lrf_manager\n", pthread_self());
    }
    
    delete data;

    return result;
}

// RemoteThread implementation

RemoteThread::RemoteThread(uint64_t id, int server_id, const std::string& func_name)
    : thread_id(id), target_server_id(server_id), status(ThreadStatus::CREATED),
      return_value(nullptr), function_name(func_name), args(nullptr) {
    // Initialize the args pointer as nullptr
    // It will be populated when the thread is created
}

RemoteThread::~RemoteThread() {
    // Cleanup any allocated return value if needed
    // Note: We don't free return_value here as it might be used by the calling thread
}

void RemoteThread::wait_for_completion() {
    std::unique_lock<std::mutex> lock(mutex);
    
    // Wait until the thread is finished or has an error
    cv.wait(lock, [this] {
        return status.load() == ThreadStatus::FINISHED || 
               status.load() == ThreadStatus::ERROR ||
               status.load() == ThreadStatus::DETACHED;
    });
}

void RemoteThread::notify_completion(void* result) {
    std::lock_guard<std::mutex> lock(mutex);
    
    return_value = result;
    status.store(ThreadStatus::FINISHED);
    
    // Notify all waiting threads
    cv.notify_all();
}

void RemoteThread::set_error() {
    std::lock_guard<std::mutex> lock(mutex);
    
    status.store(ThreadStatus::ERROR);
    
    // Notify all waiting threads
    cv.notify_all();
}

bool RemoteThread::is_finished() const {
    ThreadStatus current_status = status.load();
    return current_status == ThreadStatus::FINISHED || 
           current_status == ThreadStatus::ERROR ||
           current_status == ThreadStatus::DETACHED;
}

// ThreadManager static member initialization
std::atomic<uint64_t> ThreadManager::next_thread_id_(0);
std::unordered_map<uint64_t, std::shared_ptr<RemoteThread>> ThreadManager::remote_threads_;
std::mutex ThreadManager::threads_mutex_;

// ThreadManager implementation

uint64_t ThreadManager::allocate_thread_id() {
    // Get worker ID to ensure unique thread IDs across servers
    // Format: [WorkerID (16 bits)] [Counter (48 bits)]
    // This allows 65536 servers and 2^48 threads per server
    Worker* worker = GAllocFactory::GetWorker();
    uint64_t worker_id = worker ? worker->GetWorkerId() : 0;
    uint64_t counter = next_thread_id_.fetch_add(1);
    
    // Combine worker_id and counter
    // Keep base value, add worker_id in high bits, counter in low bits
    uint64_t thread_id = REMOTE_THREAD_ID_BASE | (worker_id << 48) | (counter & 0xFFFFFFFFFFFFULL);
    
    return thread_id;
}

void ThreadManager::register_remote_thread(std::shared_ptr<RemoteThread> thread) {
    std::lock_guard<std::mutex> lock(threads_mutex_);
    remote_threads_[thread->thread_id] = thread;
}

std::shared_ptr<RemoteThread> ThreadManager::find_remote_thread(uint64_t thread_id) {
    std::lock_guard<std::mutex> lock(threads_mutex_);
    auto it = remote_threads_.find(thread_id);
    if (it != remote_threads_.end()) {
        return it->second;
    }
    return nullptr;
}

void ThreadManager::cleanup_thread(uint64_t thread_id) {
    std::lock_guard<std::mutex> lock(threads_mutex_);
    remote_threads_.erase(thread_id);
}

bool ThreadManager::is_remote_thread_id(uint64_t thread_id) {
    return thread_id >= REMOTE_THREAD_ID_BASE;
}

void ThreadManager::initialize() {
    // Initialize the thread manager
    // Reset the thread ID counter and clear any existing threads
    next_thread_id_.store(REMOTE_THREAD_ID_BASE);
    
    std::lock_guard<std::mutex> lock(threads_mutex_);
    remote_threads_.clear();
}

void ThreadManager::cleanup() {
    std::lock_guard<std::mutex> lock(threads_mutex_);
    
    // Notify all waiting threads that the system is shutting down
    for (auto& pair : remote_threads_) {
        auto& thread = pair.second;
        thread->set_error();
    }
    
    // Clear all remote threads
    remote_threads_.clear();
}

// =============================================================================
// Remote thread implementation
// =============================================================================

int remote_pthread_create(pthread_t* thread, const pthread_attr_t* attr,
                         void* (*start_routine)(void*), void* arg, int de_target){
    // 1. Select target worker randomly (including current worker)
    int target_worker;
    if(de_target == -1)
        target_worker = select_random_worker();
    else
        target_worker = de_target;
    int current_worker_id = GAllocFactory::GetWorker()->GetWorkerId();
    
    // 2. Check if selected server is current server (local execution)
    if (target_worker == current_worker_id) {
        // Execute locally with automatic GAlloc management
        ThreadWrapperData* wrapper = new ThreadWrapperData{start_routine, arg};
        return pthread_create(thread, attr, thread_entry_wrapper, wrapper);
    }
    
    // 3. Remote execution: allocate unique remote thread ID
    uint64_t remote_thread_id = ThreadManager::allocate_thread_id();
    
    // 4. Resolve function name from function registry
    std::string func_name = resolve_function_name(start_routine);
    
    // 5. Create RemoteThread object
    auto remote_thread = std::make_shared<RemoteThread>(remote_thread_id, target_worker, func_name);
    remote_thread->args = arg;  // Store arguments
    
    // 6. Register with ThreadManager
    ThreadManager::register_remote_thread(remote_thread);
    
    // 7. Set status to running
    remote_thread->status.store(ThreadStatus::RUNNING);
    
    // 8. Return remote thread ID as pthread_t
    *thread = (pthread_t)remote_thread_id;
    
    // 9. Send message to remote server to create and execute thread
    epicLog(LOG_WARNING, "Sending remote thread create request: thread_id=%lu, target_worker=%d, function=%s", 
            remote_thread_id, target_worker, func_name.c_str());
    send_remote_thread_request(target_worker, remote_thread_id, func_name, arg);
    
    epicLog(LOG_DEBUG, "Remote pthread_create: Created remote thread %lu on server %d for function %s", 
            remote_thread_id, target_worker, func_name.c_str());
    
    return 0;

}

// =============================================================================
// Server management functions
// =============================================================================

int select_random_worker() {
    // connected_workers == total_servers;
    int total_servers = GAllocFactory::GetTotalServers();
    
    if (total_servers <= 1) {
        return 1;  // Only one worker available
    }

    // Generate random worker ID, including current worker
    std::uniform_int_distribution<int> dis(1, total_servers);
    int selected_worker = dis(gen);
    
    return selected_worker;
}

bool should_execute_remotely() {
    int total_servers = GAllocFactory::GetTotalServers();
    
    // Current policy: force all to remote if we have multiple servers
    // In the future, this could include load balancing logic, 
    // function characteristics analysis, etc.
    return (total_servers > 1);  // Execute remotely if we have multiple servers
}

std::string resolve_function_name(void* (*start_routine)(void*)) {
    // Forward declaration - implemented in gam_c_interface.cc
    extern std::string lookup_function_name(void* func_ptr);
    
    try {
        return lookup_function_name((void*)start_routine);
    } catch (const std::exception& e) {
        // If function is not registered, this is an error
    std::cerr << "Error: Function not registered for remote execution: " 
              << e.what() << std::endl;
    throw;
  }
}

// =============================================================================
// DSM pthread implementation functions (called from gam_c_interface.cc)
// =============================================================================

int dsm_pthread_create_impl(pthread_t* thread, const pthread_attr_t* attr,
                            void* (*start_routine)(void*), void* arg) {
    // Decision logic: should execute remotely?
    if (should_execute_remotely()) {
        return remote_pthread_create(thread, attr, start_routine, arg);
    } else {
        // Execute locally with automatic GAlloc management
        // Use the wrapper function from galloc_manager
        ThreadWrapperData* wrapper = new ThreadWrapperData{start_routine, arg};
        return pthread_create(thread, attr, thread_entry_wrapper, wrapper);
    }
}

int dsm_pthread_join_impl(pthread_t thread, void** retval) {
    // Check if this is a remote thread
    uint64_t thread_id = (uint64_t)thread;
    
    if (ThreadManager::is_remote_thread_id(thread_id)) {
        // Remote thread - use our custom join logic
        auto remote_thread = ThreadManager::find_remote_thread(thread_id);
        if (!remote_thread) {
            return ESRCH; // No such thread
        }
        
        // Wait for remote thread completion
        remote_thread->wait_for_completion();
        
        // Get return value
        if (retval) {
            *retval = remote_thread->return_value;
        }
        
        // Cleanup
        ThreadManager::cleanup_thread(thread_id);
        return 0;
    } else {
        // Local thread - use standard pthread_join
        return pthread_join(thread, retval);
    }
}

int dsm_pthread_detach_impl(pthread_t thread) {
    uint64_t thread_id = (uint64_t)thread;
    
    if (ThreadManager::is_remote_thread_id(thread_id)) {
        // Remote thread - mark as detached
        auto remote_thread = ThreadManager::find_remote_thread(thread_id);
        if (!remote_thread) {
            return ESRCH;
        }
        
        // Set status to detached
        remote_thread->status.store(ThreadStatus::DETACHED);
        remote_thread->cv.notify_all();
        
        // Cleanup immediately for detached threads
        ThreadManager::cleanup_thread(thread_id);
        return 0;
    } else {
        // Local thread - use standard pthread_detach
        return pthread_detach(thread);
    }
}

// =============================================================================
// Remote thread messaging functions - Integration with GAM communication
// =============================================================================

void send_remote_thread_request(int target_server_id, uint64_t thread_id, 
                               const std::string& func_name, void* arg) {
    // 1. Create WorkRequest for remote thread creation
    WorkRequest* wr = new WorkRequest();
    wr->op = REMOTE_THREAD_CREATE;
    wr->id = 0;  // Don't use id field for thread_id since it's only 32-bit
    
    // 2. Allocate buffer for thread_id + function name + arguments  
    // Buffer layout: [uint64_t thread_id][function_name_string][null_terminator][void* arg]
    size_t total_size = sizeof(uint64_t) + func_name.length() + 1 + sizeof(void*);
    char* buffer = new char[total_size];
    
    // 3. Serialize thread_id, function name and arguments
    memcpy(buffer, &thread_id, sizeof(uint64_t));
    memcpy(buffer + sizeof(uint64_t), func_name.c_str(), func_name.length());
    buffer[sizeof(uint64_t) + func_name.length()] = '\0';  // null terminator
    memcpy(buffer + sizeof(uint64_t) + func_name.length() + 1, &arg, sizeof(void*));
    
    wr->ptr = buffer;
    wr->size = total_size;  // Set correct size for the entire buffer
    
    // 4. Send through GAM communication system
    Worker* worker = GAllocFactory::GetWorker();
    Client* target_client = worker->FindClientWid(target_server_id);
    
    if (!target_client) {
        printf("Error: Cannot find target server %d for remote thread\n", target_server_id);
        zfree(buffer);  // Use zfree instead of delete[]
        delete wr;
        return;
    }
    
    // 5. Use REQUEST_SEND | REQUEST_NO_ID since we don't need immediate reply
    worker->SubmitRequest(target_client, wr, REQUEST_SEND | REQUEST_NO_ID);

    epicLog(LOG_DEBUG, "Sent remote thread request: thread_id=%lu, function=%s, arg=%p, buffer_size=%zu to server %d\n", 
           thread_id, func_name.c_str(), arg, total_size, target_server_id);
}

void send_remote_thread_completion(uint64_t thread_id, void* result, bool error) {
    // Find the mapping for this thread
    std::lock_guard<std::mutex> lock(remote_thread_mappings_mutex);
    auto it = remote_thread_mappings.find(thread_id);
    if (it == remote_thread_mappings.end()) {
        printf("Error: Cannot find mapping for completed thread %lu\n", thread_id);
        return;
    }
    
    RemoteThreadMapping& mapping = it->second;
    
    // Create WorkRequest for completion notification  
    WorkRequest* wr = new WorkRequest();
    wr->op = REMOTE_THREAD_REPLY;
    wr->id = 0;  // Don't use id field for thread_id since it's only 32-bit
    wr->status = error ? ERROR : SUCCESS;
    
    // Allocate buffer for thread_id + result pointer
    // Buffer layout: [uint64_t thread_id][void* result]
    size_t total_size = sizeof(uint64_t) + sizeof(void*);
    char* buffer = new char[total_size];
    
    // Serialize thread_id and result
    memcpy(buffer, &thread_id, sizeof(uint64_t));
    memcpy(buffer + sizeof(uint64_t), &result, sizeof(void*));
    
    wr->ptr = buffer;
    wr->size = total_size;
    
    // Send back to source client
    Worker* worker = GAllocFactory::GetWorker();
    worker->SubmitRequest(mapping.source_client, wr, REQUEST_SEND | REQUEST_NO_ID);
    
    epicLog(LOG_DEBUG, "Sent thread completion: thread_id=%lu, result=%p, error=%s\n", 
            thread_id, result, error ? "true" : "false");
    
    // Remove mapping
    remote_thread_mappings.erase(it);
}

// Global function to handle remote thread completion notification
void handle_remote_thread_completion(uint64_t thread_id, void* result, bool error) {
    auto remote_thread = ThreadManager::find_remote_thread(thread_id);
    if (remote_thread) {
        if (error) {
            remote_thread->set_error();
        } else {
            remote_thread->notify_completion(result);
        }
        epicLog(LOG_DEBUG, "Notified completion for remote thread %lu\n", thread_id);
    } else {
        epicLog(LOG_FATAL, "Could not find remote thread %lu for completion notification\n", thread_id);
    }
}

// Function to register remote thread mapping (called from ProcessRemoteThreadCreate)
void register_remote_thread_mapping(uint64_t thread_id, pthread_t local_thread, Client* source_client) {
    std::lock_guard<std::mutex> lock(remote_thread_mappings_mutex);
    // We don't actually need local_thread, just the source_client for completion notification
    RemoteThreadMapping mapping;
    mapping.source_client = source_client;
    mapping.thread_id = thread_id;
    remote_thread_mappings[thread_id] = mapping;
    // printf("Registered mapping for remote thread %lu\n", thread_id);
}

// =============================================================================
// DSM Barrier Implementation
// =============================================================================

// Compile-time check: ensure dsm_barrier_internal_t fits within pthread_barrier_t
static_assert(sizeof(dsm_barrier_internal_t) <= sizeof(pthread_barrier_t),
              "dsm_barrier_internal_t must fit within pthread_barrier_t");

int dsm_pthread_barrier_init_impl(GAddr barrier_gaddr,
                                   const pthread_barrierattr_t* attr,
                                   unsigned int count) {
    if (!barrier_gaddr || count == 0) {
        return EINVAL;
    }

    if (!g_thread_galloc) {
        g_thread_galloc = GAllocFactory::CreateAllocator();
    } 
    GAlloc* galloc = g_thread_galloc;
    if (!galloc) {
        epicLog(LOG_FATAL, "dsm_pthread_barrier_wait_impl: GAlloc is not initialized");
        return ENOMEM;
    }

    
    // Initialize barrier data structure
    dsm_barrier_internal_t init_data;
    init_data.count = count;
    init_data.waiting = 0;
    init_data.generation = 0;
    
    // Write initial data to DSM
    galloc->Write(barrier_gaddr, &init_data, sizeof(dsm_barrier_internal_t));
    
    return 0;
}

int dsm_pthread_barrier_wait_impl(GAddr barrier_gaddr) {
    if (!barrier_gaddr) {
        epicLog(LOG_FATAL, "dsm_pthread_barrier_wait_impl: barrier is NULL");
        return EINVAL;
    }
    
    
    if (!g_thread_galloc) {
        g_thread_galloc = GAllocFactory::CreateAllocator();
    } 
    GAlloc* galloc = g_thread_galloc;
    if (!galloc) {
        epicLog(LOG_FATAL, "dsm_pthread_barrier_wait_impl: GAlloc is not initialized");
        return ENOMEM;
    }
    
    dsm_barrier_internal_t barrier_data;
    
    // Use WLock to get exclusive access
    galloc->WLock(barrier_gaddr, sizeof(dsm_barrier_internal_t));
    
    // Read current barrier state
    galloc->Read(barrier_gaddr, &barrier_data, sizeof(dsm_barrier_internal_t));
    
    unsigned int my_generation = barrier_data.generation;
    barrier_data.waiting++;
    
    
    if (barrier_data.waiting == barrier_data.count) {
        // Last thread to arrive: reset counter, increment generation
        barrier_data.waiting = 0;
        barrier_data.generation++;
        
        // Write back and release lock
        galloc->Write(barrier_gaddr, &barrier_data, sizeof(dsm_barrier_internal_t));
        galloc->UnLock(barrier_gaddr, sizeof(dsm_barrier_internal_t));
        
        epicLog(LOG_DEBUG, "dsm_pthread_barrier_wait_impl: Last thread released barrier , new gen=%u",
                barrier_data.generation);
        
        return PTHREAD_BARRIER_SERIAL_THREAD;
    } else {
        // Write back wait count and release lock
        galloc->Write(barrier_gaddr, &barrier_data, sizeof(dsm_barrier_internal_t));
        galloc->UnLock(barrier_gaddr, sizeof(dsm_barrier_internal_t));
        
        // Spin-wait for generation change - using exponential backoff strategy
        unsigned int current_generation;
        int spin_count = 0;
        const int SPIN_THRESHOLD = 1000000000;  // Busy-wait threshold, adjustable per system
        
        do {
            // Early phase: use busy-wait (user-space polling)
            if (spin_count < SPIN_THRESHOLD) {
                // CPU pause instruction to reduce power consumption and contention
                #if defined(__x86_64__) || defined(__i386__)
                asm volatile("pause" ::: "memory");
                #elif defined(__aarch64__)
                asm volatile("yield" ::: "memory");
                #endif
            } else {
                // Late phase: yield CPU to avoid long-term occupation
                sched_yield();
            }
            
            spin_count++;
            
            // Use RLock for reading
            galloc->RLock(barrier_gaddr, sizeof(dsm_barrier_internal_t));
            galloc->Read(barrier_gaddr, &barrier_data, sizeof(dsm_barrier_internal_t));
            current_generation = barrier_data.generation;
            galloc->UnLock(barrier_gaddr, sizeof(dsm_barrier_internal_t));
            
        } while (current_generation == my_generation);
        
        
        return 0;
    }
}
