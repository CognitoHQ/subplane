#include <atomic>
#include <iostream>
#include <string>
#include <signal.h>
#include <memory>
#include <thread>
#include "subplane/agent.h"

using namespace subplane;

std::unique_ptr<Agent> g_agent;
std::atomic<int> g_signal{0};
static_assert(std::atomic<int>::is_always_lock_free);

// INTENT: only an async-signal-safe store. The handler used to print and call
// Agent::stop() itself; a SIGTERM that landed while the main thread held the
// agent's mutex (inside start(), or inside malloc) deadlocked that thread on
// itself, and the agent never exited. The main thread acts on the flag.
void signal_handler(int signal) {
    g_signal.store(signal, std::memory_order_relaxed);
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]\n";
    std::cout << "Options:\n";
    std::cout << "  --config <path>    Configuration file path (default: config.yaml)\n";
    std::cout << "  --help            Show this help message\n";
    std::cout << "  --version         Show version information\n";
}

int main(int argc, char* argv[]) {
    std::string config_path = "config.yaml";
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--version" || arg == "-v") {
            std::cout << "subplaned version 2.0.0-dev\n";
            return 0;
        } else if (arg == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }
    
    // Set up signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    try {
        // Create and initialize agent
        g_agent = std::make_unique<Agent>();
        
        std::cout << "Initializing subplaned..." << std::endl;
        if (!g_agent->initialize(config_path)) {
            std::cerr << "Failed to initialize agent" << std::endl;
            return 1;
        }
        
        if (g_signal.load(std::memory_order_relaxed) != 0) return 0;   // asked to stop while initializing

        std::cout << "Starting subplane agent..." << std::endl;
        if (!g_agent->start()) {
            std::cerr << "Failed to start agent" << std::endl;
            return 1;
        }
        
        std::cout << "subplane agent is running. Press Ctrl+C to stop." << std::endl;
        
        // Wait for shutdown signal
        while (g_agent->is_running()) {
            if (const int sig = g_signal.load(std::memory_order_relaxed); sig != 0) {
                std::cout << "\nReceived signal " << sig << ", shutting down..." << std::endl;
                g_agent->stop();
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        std::cout << "subplane agent stopped." << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
