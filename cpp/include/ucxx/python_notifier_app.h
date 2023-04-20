/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#pragma once

#include <chrono>
#include <future>
#include <memory>
#include <queue>
#include <sstream>
#include <vector>

#include <iostream>

#include <Python.h>

#include <ucxx/log.h>
#include <ucxx/python/future.h>

namespace python_notifier {

class PythonFuture : public std::enable_shared_from_this<PythonFuture> {
 private:
  PyObject* _asyncioEventLoop{};  ///< The handle to the Python future
  PyObject* _handle{};            ///< The handle to the Python future

 public:
  explicit PythonFuture(PyObject* asyncioEventLoop)
    : _asyncioEventLoop(asyncioEventLoop),
      _handle{ucxx::python::create_python_future(asyncioEventLoop)}
  {
  }
  PythonFuture(const PythonFuture&) = delete;
  PythonFuture& operator=(PythonFuture const&) = delete;
  PythonFuture(PythonFuture&& o)               = delete;
  PythonFuture& operator=(PythonFuture&& o) = delete;

  ~PythonFuture() { Py_XDECREF(_handle); }

  void set(size_t result)
  {
    if (_handle == nullptr) throw std::runtime_error("Invalid object or already released");

    ucxx_warn("PythonFuture::set() this: %p, _handle: %p, result: %llu", this, _handle, result);
    if (result >= 0) {
      // PyLong_FromSize_t requires the GIL
      PyGILState_STATE state = PyGILState_Ensure();
      ucxx::python::future_set_result(_asyncioEventLoop, _handle, PyLong_FromSize_t(result));
      PyGILState_Release(state);
    } else {
      std::stringstream sstream;
      sstream << "Error on task " << -result;
      ucxx::python::future_set_exception(
        _asyncioEventLoop, _handle, PyExc_RuntimeError, sstream.str().c_str());
    }
  }

  PyObject* getHandle()
  {
    if (_handle == nullptr) throw std::runtime_error("Invalid object or already released");

    return _handle;
  }

  PyObject* release()
  {
    if (_handle == nullptr) throw std::runtime_error("Invalid object or already released");

    return std::exchange(_handle, nullptr);
  }
};

typedef std::shared_ptr<PythonFuture> PythonFuturePtr;

typedef std::future<size_t> CppFuture;
typedef std::shared_ptr<CppFuture> CppFuturePtr;

struct Task {
  CppFuturePtr future{nullptr};
  PythonFuturePtr pythonFuture{nullptr};
  double duration = 1.0;
  size_t id       = 0;
};

typedef std::vector<Task> TaskPool;
typedef std::shared_ptr<TaskPool> TaskPoolPtr;

class ApplicationThread {
 private:
  std::thread _thread{};  ///< Thread object
  bool _stop{false};      ///< Signal to stop on next iteration

 public:
  ApplicationThread(PyObject* asyncioEventLoop,
                    std::shared_ptr<std::mutex> incomingPoolMutex,
                    TaskPoolPtr incomingPool,
                    TaskPoolPtr readyPool,
                    std::function<void(PyObject*)> populateFunction)
  {
    ucxx_warn("Starting application thread");
    _thread = std::thread(ApplicationThread::progressUntilSync,
                          asyncioEventLoop,
                          incomingPoolMutex,
                          incomingPool,
                          readyPool,
                          populateFunction,
                          std::ref(_stop));
  }

  ~ApplicationThread()
  {
    ucxx_warn("~ApplicationThread");
    if (!_thread.joinable()) {
      ucxx_warn("Application thread not running or already stopped");
      return;
    }

    _stop = true;
    _thread.join();
  }

  static void submit(std::shared_ptr<std::mutex> incomingPoolMutex,
                     TaskPoolPtr incomingPool,
                     TaskPoolPtr processingPool)
  {
    // ucxx_warn("Application submitting %lu tasks", incomingPool->size());
    std::lock_guard<std::mutex> lock(incomingPoolMutex.get());
    for (auto it = incomingPool->begin(); it != incomingPool->end();) {
      auto& task  = *it;
      task.future = std::make_shared<CppFuture>(std::async(std::launch::async, [&]() {
        std::this_thread::sleep_for(std::chrono::duration<double>(task.duration));
        return task.id;
      }));
      processingPool->push_back(task);
      it = incomingPool->erase(it);
    }
  }

  static void processLoop(TaskPoolPtr processingPool, TaskPoolPtr readyPool)
  {
    // ucxx_warn("Processing %lu tasks", processingPool->size());
    for (auto it = processingPool->begin(); it != processingPool->end();) {
      auto& task = *it;
      // 50 ms
      std::future_status status = task.future->wait_for(std::chrono::duration<double>(0.05));
      if (status == std::future_status::ready) {
        ucxx_warn("Task %llu ready", task.id);
        // TODO: generalized implementation required instead of always taking `ucs_status_t`
        task.pythonFuture->set(task.id);
        readyPool->push_back(task);
        it = processingPool->erase(it);
      }
      // else {
      //   if (status == std::future_status::deferred)
      //     ucxx_warn("Task %llu deferred", task.id);
      //   else if (status == std::future_status::deferred)
      //     ucxx_warn("Task %llu timed out", task.id)
      // }
    }
  }

  static void progressUntilSync(PyObject* asyncioEventLoop,
                                std::shared_ptr<std::mutex> incomingPoolMutex,
                                TaskPoolPtr incomingPool,
                                TaskPoolPtr readyPool,
                                std::function<void(PyObject*)> populateFunction,
                                const bool& stop)
  {
    ucxx_warn("Application thread started");
    auto processingPool = std::make_shared<TaskPool>();
    while (!stop) {
      populateFunction(asyncioEventLoop);
      // ucxx_warn("Application thread loop");
      ApplicationThread::submit(incomingPoolMutex, incomingPool, processingPool);
      ApplicationThread::processLoop(processingPool, readyPool);
    }
  }
};

class Application {
 private:
  std::unique_ptr<ApplicationThread> _thread{nullptr};  ///< The progress thread object
  std::mutex _incomingPoolMutex{};                      ///< Mutex to access the Python futures pool
  TaskPoolPtr _incomingPool{std::make_shared<TaskPool>()};  ///< Incoming task pool
  TaskPoolPtr _readyPool{
    std::make_shared<TaskPool>()};       ///< Ready task pool, only to ensure task lifetime
  std::mutex _pythonFuturesPoolMutex{};  ///< Mutex to access the Python futures pool
  std::queue<std::shared_ptr<PythonFuture>>
    _pythonFuturesPool{};  ///< Python futures pool to prevent running out of fresh futures
  PyObject* _asyncioEventLoop{nullptr};

  std::shared_ptr<PythonFuture> getPythonFuture()
  {
    if (_pythonFuturesPool.size() == 0) {
      ucxx_warn(
        "No Python Futures available during getPythonFuture(), make sure the "
        "Notifier Thread is running and calling populatePythonFuturesPool() "
        "periodically. Filling futures pool now, but this is inefficient.");
      populatePythonFuturesPool(_asyncioEventLoop);
    }

    PythonFuturePtr ret{nullptr};
    {
      std::lock_guard<std::mutex> lock(_pythonFuturesPoolMutex);
      ret = _pythonFuturesPool.front();
      _pythonFuturesPool.pop();
    }
    ucxx_warn("getPythonFuture: %p %p", ret.get(), ret->getHandle());
    return ret;
  }

 public:
  explicit Application(PyObject* asyncioEventLoop) : _asyncioEventLoop(asyncioEventLoop)
  {
    ucxx::parseLogLevel();

    ucxx_warn("Launching application");

    auto populateFunction =
      std::bind(&Application::populatePythonFuturesPool, this, std::placeholders::_1);

    _thread = std::make_unique<ApplicationThread>(
      _asyncioEventLoop, _incomingPoolMutex, _incomingPool, _readyPool, populateFunction);
  }

  void populatePythonFuturesPool(PyObject* asyncioEventLoop)
  {
    // ucxx_warn("populatePythonFuturesPool");
    // If the pool goes under half expected size, fill it up again.
    if (_pythonFuturesPool.size() < 50) {
      std::lock_guard<std::mutex> lock(_pythonFuturesPoolMutex);
      while (_pythonFuturesPool.size() < 100)
        _pythonFuturesPool.emplace(std::make_shared<PythonFuture>(asyncioEventLoop));
    }
  }

  PyObject* submit(double duration = 1.0, size_t id = 0)
  {
    ucxx_warn("Submitting task with id: %llu, duration: %f", id, duration);
    Task task = {.pythonFuture = getPythonFuture(), .duration = duration, .id = id};

    {
      std::lock_guard<std::mutex> lock(_incomingPoolMutex);
      _incomingPool->push_back(task);
    }

    return task.pythonFuture->getHandle();
  }
};

}  // namespace python_notifier
