#pragma once

#include <iostream>
#include <vector>
#include <queue>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>

// --- Thread-safe queue ---
template<typename T>
class ThreadSafeQueue {
  std::queue<T> q;
  std::mutex m;
  std::condition_variable cv;

public:
  void push(T value) {
    {
      std::lock_guard<std::mutex> lock(m);
      q.push(std::move(value));
    }
    cv.notify_one();
  }

  bool tryPop(T& result) {
    std::unique_lock<std::mutex> lock(m);
    if (q.empty()) return false;
    result = std::move(q.front());
    q.pop();
    return true;
  }

  bool waitPop(T& result) {
    std::unique_lock<std::mutex> lock(m);
    cv.wait(lock, [&] { return !q.empty(); });
    result = std::move(q.front());
    q.pop();
    return true;
  }
};
