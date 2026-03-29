#include <string>
#include <unordered_map>
#include <mutex>
#include <stdexcept>
#include <cstdio>

// =============================================================================
// Function Registry System (Internal C++ functions)
// =============================================================================

// Global function registry maps
static std::unordered_map<void*, std::string> function_pointer_to_name;
static std::unordered_map<std::string, void*> function_name_to_pointer;
static std::mutex function_registry_mutex;

// Internal function registry interface (C++)
std::string lookup_function_name(void* func_ptr) {
    std::lock_guard<std::mutex> lock(function_registry_mutex);
    
    auto it = function_pointer_to_name.find(func_ptr);
    if (it != function_pointer_to_name.end()) {
        return it->second;
    }
    
    throw std::runtime_error("Function not registered for remote execution");
}

void* lookup_function_pointer(const std::string& func_name) {
    std::lock_guard<std::mutex> lock(function_registry_mutex);
    
    auto it = function_name_to_pointer.find(func_name);
    if (it != function_name_to_pointer.end()) {
        return it->second;
    }
    
    return nullptr;  // Function not found
}

// C interface for function registration
extern "C" {

void register_remote_function(const char* name, void* (*func_ptr)(void*)) {
    std::lock_guard<std::mutex> lock(function_registry_mutex);
    
    std::string func_name(name);
    function_pointer_to_name[(void*)func_ptr] = func_name;
    function_name_to_pointer[func_name] = (void*)func_ptr;
    
    printf("Registered function: %s -> %p\n", name, (void*)func_ptr);
}

}
