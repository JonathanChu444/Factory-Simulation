# Factory-Simulation

A multithreaded C++ simulation in which producers create packages, couriers transport them to a warehouse along a shared outbound road, and workers process them. Each courier returns on a separate lane.

This project was used to practice thread synchronization using mutexes, condition variables, atomic variables, and monitor-style classes.

compile: g++ -std=c++17 -pthread -Wall -Wextra -Wpedantic warehouse.cpp -o warehouse
run: ./warehouse