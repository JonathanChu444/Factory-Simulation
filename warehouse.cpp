// warehouse.cpp
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace std::chrono_literals;

struct Position {
    int x = 0;
    int y = 0;
};

struct Package {
    int id;
};

// Protects a bounded package queue.
class WarehouseMonitor {
public:
    explicit WarehouseMonitor(std::size_t capacity)
        : capacity_(capacity) {}

    // Block while the warehouse is full.
    // Return false if shutdown occurs before insertion.
    bool deposit(Package package, const std::atomic<bool>& running) {
        std::unique_lock<std::mutex> lock(mutex_);
        notFull_.wait(lock, [&]{ return packages_.size() < capacity_ || !running.load(std::memory_order_relaxed); });
        if (!running.load(std::memory_order_relaxed)) {
            return false;
        }
        packages_.push(package);
        lock.unlock();
        notEmpty_.notify_one();
        return true;
    }

    // Block while the warehouse is empty.
    // Return false if shutdown occurs before removal.
    bool withdraw(Package& result, const std::atomic<bool>& running) {
        std::unique_lock<std::mutex> lock(mutex_);
        notEmpty_.wait(lock, [&]{return packages_.size() > 0 || !running.load(std::memory_order_relaxed); });
        if (!running.load(std::memory_order_relaxed) && packages_.size() == 0) {
            return false;
        }
        result = packages_.front();
        packages_.pop();
        lock.unlock();
        notFull_.notify_all();
        return true;
    }

    std::size_t packageCount() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return packages_.size();
    }

    // Wake threads so they can observe running == false.
    void notifyShutdown() {
        notEmpty_.notify_all();
        notFull_.notify_all();
    }

private:
    const std::size_t capacity_;
    std::queue<Package> packages_;

    mutable std::mutex mutex_;
    std::condition_variable notEmpty_;
    std::condition_variable notFull_;
};

// Prevents multiple couriers from occupying the same coordinate.
class TrafficMonitor {
public:
    void addCourier(int courierId, Position initial) {
        std::unique_lock<std::mutex> lock(mutex_);
        positions_.insert({courierId, initial});
    }

    // Block until the requested position is unoccupied.
    // Return false if shutdown occurs.
    bool moveTo(
        int courierId,
        Position destination,
        const std::atomic<bool>& running
    ) {
        std::unique_lock<std::mutex> lock(mutex_);
        positionChanged_.wait(lock, [&] {
            return positionIsFree(destination, courierId) ||
                   !running.load(std::memory_order_relaxed);
        });
        if (!running.load(std::memory_order_relaxed)) {
            return false;
        }
        positions_[courierId] = destination;
        lock.unlock();
        positionChanged_.notify_all();  
        return true;
    }

    std::unordered_map<int, Position> snapshot() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return positions_;
    }

    void notifyShutdown() {
        positionChanged_.notify_all();
    }

private:
    bool positionIsFree(Position target, int movingCourier) const {
        for (auto& [courierId, position] : positions_) {
            if (courierId != movingCourier &&
                position.x == target.x && position.y == target.y) {
                return false;
            }
        }
        return true;
    }

    mutable std::mutex mutex_;
    std::condition_variable positionChanged_;
    std::unordered_map<int, Position> positions_;
};

struct Statistics {
    int produced = 0;
    int deposited = 0;
    int consumed = 0;
};

// Monitor for related statistics.
class StatisticsMonitor {
public:
    void recordProduced() {
        std::unique_lock<std::mutex> lock(mutex_);
        stats_.produced++;
    }

    void recordDeposited() {
        std::unique_lock<std::mutex> lock(mutex_);
        stats_.deposited++;
    }

    void recordConsumed() {
        std::unique_lock<std::mutex> lock(mutex_);
        stats_.consumed++;
    }

    Statistics snapshot() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return stats_;
    }

private:
    mutable std::mutex mutex_;
    Statistics stats_;
};

void producerLoop(
    int producerId,
    std::atomic<int>& nextPackageId,
    std::queue<Package>& loadingArea,
    std::mutex& loadingMutex,
    std::condition_variable& packageReady,
    StatisticsMonitor& statistics,
    const std::atomic<bool>& running
) {
    while (running.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(150ms);
        int packageId = nextPackageId.fetch_add(1, std::memory_order_relaxed);
        std::unique_lock<std::mutex> lock(loadingMutex);
        loadingArea.push(Package{packageId});
        lock.unlock();
        statistics.recordProduced();
        packageReady.notify_one();
    }

    std::cout << "Producer " << producerId << " stopped\n";
}

void courierLoop(
    int courierId,
    WarehouseMonitor& warehouse,
    TrafficMonitor& traffic,
    std::queue<Package>& loadingArea,
    std::mutex& loadingMutex,
    std::condition_variable& packageReady,
    StatisticsMonitor& statistics,
    const std::atomic<bool>& running
) {
    while (running.load(std::memory_order_relaxed)) {
        Package package{-1};

        std::unique_lock<std::mutex> lock(loadingMutex);
        packageReady.wait(lock, [&]{ return !loadingArea.empty() || !running; });
        if (!running && loadingArea.empty()) {
            break;
        }
        package = loadingArea.front();
        loadingArea.pop();
        lock.unlock();

        // Move along a shared one-dimensional road.
        for (int x = 1; x <= 4; ++x) {
            if (!traffic.moveTo(courierId, {x, 0}, running)) {
                return;
            }

            std::this_thread::sleep_for(80ms);
        }

        if (!warehouse.deposit(package, running)) {
            return;
        }

        statistics.recordDeposited();

        // Return to the loading area.
        for (int x = 3; x >= 0; --x) {
            if (!traffic.moveTo(courierId, {x, courierId + 1}, running)) {
                return;
            }

            std::this_thread::sleep_for(80ms);
        }
    }
}

void workerLoop(
    int workerId,
    WarehouseMonitor& warehouse,
    StatisticsMonitor& statistics,
    const std::atomic<bool>& running
) {
    while (true) {
        Package package{-1};

        if (!warehouse.withdraw(package, running)) {
            break;
        }

        std::cout << "Worker " << workerId
                  << " processing package " << package.id << '\n';

        std::this_thread::sleep_for(300ms);
        statistics.recordConsumed();
    }
}

void displayLoop(
    const WarehouseMonitor& warehouse,
    const TrafficMonitor& traffic,
    const StatisticsMonitor& statistics,
    const std::atomic<bool>& running
) {
    while (running.load(std::memory_order_relaxed)) {
        // Obtain independent, thread-safe snapshots.
        Statistics stats = statistics.snapshot();
        auto positions = traffic.snapshot();

        std::cout << "\n--- Snapshot ---\n"
                  << "Produced:  " << stats.produced << '\n'
                  << "Deposited: " << stats.deposited << '\n'
                  << "Consumed:  " << stats.consumed << '\n'
                  << "In stock:  " << warehouse.packageCount() << '\n';

        for (const auto& [id, position] : positions) {
            std::cout << "Courier " << id << ": ("
                      << position.x << ", " << position.y << ")\n";
        }

        std::this_thread::sleep_for(500ms);
    }
}

int main() {
    std::atomic<bool> running{true};
    std::atomic<int> nextPackageId{0};

    WarehouseMonitor warehouse(5);
    TrafficMonitor traffic;
    StatisticsMonitor statistics;

    std::queue<Package> loadingArea;
    std::mutex loadingMutex;
    std::condition_variable packageReady;

    traffic.addCourier(0, {0, 0});
    traffic.addCourier(1, {0, 1});
    int worker_count = 4;
    int courier_count = 2;

    std::vector<std::thread> threads;

    threads.emplace_back(
        producerLoop,
        0,
        std::ref(nextPackageId),
        std::ref(loadingArea),
        std::ref(loadingMutex),
        std::ref(packageReady),
        std::ref(statistics),
        std::cref(running)
    );

    for (int id = 0; id < worker_count; ++id) {
        threads.emplace_back(
            courierLoop,
            id,
            std::ref(warehouse),
            std::ref(traffic),
            std::ref(loadingArea),
            std::ref(loadingMutex),
            std::ref(packageReady),
            std::ref(statistics),
            std::cref(running)
        );
    }

    for (int id = 0; id < courier_count; ++id) {
        threads.emplace_back(
            workerLoop,
            id,
            std::ref(warehouse),
            std::ref(statistics),
            std::cref(running)
        );
    }

    std::thread displayThread(
        displayLoop,
        std::cref(warehouse),
        std::cref(traffic),
        std::cref(statistics),
        std::cref(running)
    );

    std::this_thread::sleep_for(10s);

    // Begin clean shutdown.
    running.store(false, std::memory_order_relaxed);

    // Wake every thread that might be sleeping on a condition variable.
    warehouse.notifyShutdown();
    traffic.notifyShutdown();
    packageReady.notify_all();

    for (std::thread& thread : threads) {
        thread.join();
    }

    displayThread.join();

    Statistics finalStats = statistics.snapshot();

    std::cout << "\nFinal totals:\n"
              << "Produced: " << finalStats.produced << '\n'
              << "Deposited: " << finalStats.deposited << '\n'
              << "Consumed: " << finalStats.consumed << '\n';
}