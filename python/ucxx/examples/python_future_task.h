/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#pragma once

#include <ucxx/log.h>
#include <ucxx/python/python_future_task.h>

#include <chrono>
#include <future>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <Python.h>

namespace ucxx {

namespace python_future_task {

typedef std::shared_ptr<PythonFutureTask<size_t>> PythonFutureTaskPtr;
typedef std::vector<PythonFutureTaskPtr> FuturePool;
typedef std::shared_ptr<FuturePool> FuturePoolPtr;

class ApplicationThread {
 private:
  std::thread _thread{};  ///< Thread object
  bool _stop{false};      ///< Signal to stop on next iteration

 public:
  ApplicationThread(PyObject* asyncioEventLoop,
                    std::shared_ptr<std::mutex> incomingPoolMutex,
                    FuturePoolPtr incomingPool,
                    FuturePoolPtr readyPool) {
    ucxx_warn("Starting application thread");
    _thread = std::thread(ApplicationThread::progressUntilSync,
                          asyncioEventLoop,
                          incomingPoolMutex,
                          incomingPool,
                          readyPool,
                          std::ref(_stop));
  }

  ~ApplicationThread() {
    ucxx_warn("~ApplicationThread");
    if (!_thread.joinable()) {
      ucxx_warn("Application thread not running or already stopped");
      return;
    }

    _stop = true;
    _thread.join();
  }

  static void submit(std::shared_ptr<std::mutex> incomingPoolMutex,
                     FuturePoolPtr incomingPool,
                     FuturePoolPtr processingPool) {
    // ucxx_warn("Application submitting %lu tasks", incomingPool->size());
    std::lock_guard<std::mutex> lock(*incomingPoolMutex);
    for (auto it = incomingPool->begin(); it != incomingPool->end();) {
      auto& task = *it;
      processingPool->push_back(task);
      it = incomingPool->erase(it);
    }
  }

  static void processLoop(FuturePoolPtr processingPool, FuturePoolPtr readyPool) {
    // ucxx_warn("Processing %lu tasks", processingPool->size());
    while (!processingPool->empty()) {
      for (auto it = processingPool->begin(); it != processingPool->end();) {
        auto& task   = *it;
        auto& future = task->getFuture();

        // 10 ms
        std::future_status status = future.wait_for(std::chrono::duration<double>(0.01));
        if (status == std::future_status::ready) {
          ucxx_warn("Task %llu ready", future.get());
          readyPool->push_back(task);
          it = processingPool->erase(it);
          continue;
        }

        ++it;
      }
    }
  }

  static void progressUntilSync(PyObject* asyncioEventLoop,
                                std::shared_ptr<std::mutex> incomingPoolMutex,
                                FuturePoolPtr incomingPool,
                                FuturePoolPtr readyPool,
                                const bool& stop) {
    ucxx_warn("Application thread started");
    auto processingPool = std::make_shared<FuturePool>();
    while (!stop) {
      // ucxx_warn("Application thread loop");
      ApplicationThread::submit(incomingPoolMutex, incomingPool, processingPool);
      ApplicationThread::processLoop(processingPool, readyPool);
    }
  }
};

class Application {
 private:
  std::unique_ptr<ApplicationThread> _thread{nullptr};  ///< The progress thread object
  std::shared_ptr<std::mutex> _incomingPoolMutex{
    std::make_shared<std::mutex>()};  ///< Mutex to access the Python futures pool
  FuturePoolPtr _incomingPool{std::make_shared<FuturePool>()};  ///< Incoming task pool
  FuturePoolPtr _readyPool{
    std::make_shared<FuturePool>()};  ///< Ready task pool, only to ensure task lifetime
  PyObject* _asyncioEventLoop{nullptr};

 public:
  explicit Application(PyObject* asyncioEventLoop) : _asyncioEventLoop(asyncioEventLoop) {
    ucxx::parseLogLevel();

    ucxx_warn("Launching application");

    _thread = std::make_unique<ApplicationThread>(
      _asyncioEventLoop, _incomingPoolMutex, _incomingPool, _readyPool);
  }

  PyObject* submit(double duration = 1.0, size_t id = 0) {
    ucxx_warn("Submitting task with id: %llu, duration: %f", id, duration);
    auto task = std::make_shared<PythonFutureTask<size_t>>(
      std::packaged_task<size_t()>([duration, id]() {
        ucxx_warn("Task with id %llu sleeping for %f", id, duration);
        std::this_thread::sleep_for(std::chrono::duration<double>(duration));
        return id;
      }),
      PyLong_FromSize_t,
      _asyncioEventLoop);

    {
      std::lock_guard<std::mutex> lock(*_incomingPoolMutex);
      _incomingPool->push_back(task);
    }

    return task->getHandle();
  }
};

}  // namespace python_future_task
//
}  // namespace ucxx
