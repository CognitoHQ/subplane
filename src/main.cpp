#include <iostream>
#include <string>
#include <signal.h>
#include <memory>
#include <thread>
#include "flashtick/agent.h"

using namespace flashtick;

std::unique_ptr<Agent> g_agent;

void signal_handler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down..." << std::endl;
    if (g_agent) {
        g_agent->stop();
    }
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
            std::cout << "flashtickd version 2.0.0-dev\n";
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
        
        std::cout << "Initializing flashtickd..." << std::endl;
        if (!g_agent->initialize(config_path)) {
            std::cerr << "Failed to initialize agent" << std::endl;
            return 1;
        }
        
        std::cout << "Starting flashtick agent..." << std::endl;
        if (!g_agent->start()) {
            std::cerr << "Failed to start agent" << std::endl;
            return 1;
        }
        
        std::cout << "flashtick agent is running. Press Ctrl+C to stop." << std::endl;
        
        // Wait for shutdown signal
        while (g_agent->is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        std::cout << "flashtick agent stopped." << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
