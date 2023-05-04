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
#include <string>
#include <utility>
#include <vector>

#include <iostream>

#include <Python.h>

#include <ucxx/log.h>
#include <ucxx/python/future.h>

namespace python_notifier {

template <typename ReturnType, typename... Args>
class PythonFuture : public std::enable_shared_from_this<PythonFuture<ReturnType>> {
 private:
  std::packaged_task<ReturnType(Args...)> _task{};  ///< The user-defined C++ task to run
  std::function<PyObject*(ReturnType)>
    _pythonConvert{};                 ///< Function to convert the C++ result into Python value
  PyObject* _asyncioEventLoop{};      ///< The handle to the Python asyncio event loop
  PyObject* _handle{};                ///< The handle to the Python future
  std::future<ReturnType> _future{};  ///< The C++ future containing the task result

 public:
  explicit PythonFuture(std::packaged_task<ReturnType(Args...)> task,
                        std::function<PyObject*(ReturnType)> pythonConvert,
                        PyObject* asyncioEventLoop)
    : _task{std::move(task)},
      _pythonConvert(pythonConvert),
      _asyncioEventLoop(asyncioEventLoop),
      _handle{ucxx::python::create_python_future(asyncioEventLoop)}
  {
    _future = std::async(std::launch::async, [this]() {
      std::future<ReturnType> result = this->_task.get_future();
      this->_task();
      try {
        const ReturnType r = result.get();
        this->_setValue(r);
        return r;
      } catch (std::exception& e) {
        this->_setException(e);
      }
    });
  }
  PythonFuture(const PythonFuture&) = delete;
  PythonFuture& operator=(PythonFuture const&) = delete;
  PythonFuture(PythonFuture&& o)               = delete;
  PythonFuture& operator=(PythonFuture&& o) = delete;

  ~PythonFuture() { Py_XDECREF(_handle); }

  void _setValue(const ReturnType result)
  {
    // PyLong_FromSize_t requires the GIL
    if (_handle == nullptr) throw std::runtime_error("Invalid object or already released");
    PyGILState_STATE state = PyGILState_Ensure();
    ucxx::python::future_set_result(_asyncioEventLoop, _handle, _pythonConvert(result));
    PyGILState_Release(state);
  }

  void _setPythonException(PyObject* pythonException, const std::string& message)
  {
    if (_handle == nullptr) throw std::runtime_error("Invalid object or already released");
    ucxx::python::future_set_exception(
      _asyncioEventLoop, _handle, pythonException, message.c_str());
  }

  void _setException(const std::exception& exception)
  {
    try {
      throw exception;
    } catch (const std::bad_alloc& e) {
      _setPythonException(PyExc_MemoryError, e.what());
    } catch (const std::bad_cast& e) {
      _setPythonException(PyExc_TypeError, e.what());
    } catch (const std::bad_typeid& e) {
      _setPythonException(PyExc_TypeError, e.what());
    } catch (const std::domain_error& e) {
      _setPythonException(PyExc_ValueError, e.what());
    } catch (const std::invalid_argument& e) {
      _setPythonException(PyExc_ValueError, e.what());
    } catch (const std::ios_base::failure& e) {
      _setPythonException(PyExc_IOError, e.what());
    } catch (const std::out_of_range& e) {
      _setPythonException(PyExc_IndexError, e.what());
    } catch (const std::overflow_error& e) {
      _setPythonException(PyExc_OverflowError, e.what());
    } catch (const std::range_error& e) {
      _setPythonException(PyExc_ArithmeticError, e.what());
    } catch (const std::underflow_error& e) {
      _setPythonException(PyExc_ArithmeticError, e.what());
    } catch (const std::exception& e) {
      _setPythonException(PyExc_RuntimeError, e.what());
    } catch (...) {
      _setPythonException(PyExc_RuntimeError, "Unknown exception");
    }
  }

  std::future<ReturnType>& getFuture() { return _future; }

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

typedef std::shared_ptr<PythonFuture<size_t>> PythonFuturePtr;

typedef std::vector<PythonFuturePtr> FuturePool;
typedef std::shared_ptr<FuturePool> FuturePoolPtr;

class ApplicationThread {
 private:
  std::thread _thread{};  ///< Thread object
  bool _stop{false};      ///< Signal to stop on next iteration

 public:
  ApplicationThread(PyObject* asyncioEventLoop,
                    std::shared_ptr<std::mutex> incomingPoolMutex,
                    FuturePoolPtr incomingPool,
                    FuturePoolPtr readyPool)
  {
    ucxx_warn("Starting application thread");
    _thread = std::thread(ApplicationThread::progressUntilSync,
                          asyncioEventLoop,
                          incomingPoolMutex,
                          incomingPool,
                          readyPool,
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
                     FuturePoolPtr incomingPool,
                     FuturePoolPtr processingPool)
  {
    // ucxx_warn("Application submitting %lu tasks", incomingPool->size());
    std::lock_guard<std::mutex> lock(*incomingPoolMutex);
    for (auto it = incomingPool->begin(); it != incomingPool->end();) {
      auto& task = *it;
      processingPool->push_back(task);
      it = incomingPool->erase(it);
    }
  }

  static void processLoop(FuturePoolPtr processingPool, FuturePoolPtr readyPool)
  {
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
                                const bool& stop)
  {
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
  explicit Application(PyObject* asyncioEventLoop) : _asyncioEventLoop(asyncioEventLoop)
  {
    ucxx::parseLogLevel();

    ucxx_warn("Launching application");

    _thread = std::make_unique<ApplicationThread>(
      _asyncioEventLoop, _incomingPoolMutex, _incomingPool, _readyPool);
  }

  PyObject* submit(double duration = 1.0, size_t id = 0)
  {
    ucxx_warn("Submitting task with id: %llu, duration: %f", id, duration);
    auto task = std::make_shared<PythonFuture<size_t>>(
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

}  // namespace python_notifier
