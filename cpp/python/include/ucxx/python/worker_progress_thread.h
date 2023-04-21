/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include <ucxx/delayed_submission.h>
#include <ucxx/worker_progress_thread.h>

namespace ucxx {

namespace python {

typedef std::function<void(void)> PopulatePythonFuturesFunction;
typedef std::function<void(void)> RequestNotifierFunction;

class WorkerProgressThread : public ::ucxx::WorkerProgressThread {
 private:
  PopulatePythonFuturesFunction _populatePythonFuturesFunction;
  RequestNotifierFunction _requestNotifierFunction;

  /**
   * @brief The function executed in the new thread.
   *
   * This function ensures the `startCallback` is executed once at the start of the thread,
   * subsequently starting a continuous loop that processes any delayed submission requests
   * that are pending in the `delayedSubmissionCollection` followed by the execution of the
   * `progressFunction`, the loop repeats until `stop` is set.
   *
   * @param[in] progressFunction              user-defined progress function implementation.
   * @param[in] populatePythonFuturesFunction user-defined function implementation to
   *                                          populate a Python future pool.
   * @param[in] requestNotifierFunction       user-defined function implementation to
   *                                          handle request notification.
   * @param[in] stop                          reference to the stop signal causing the
   *                                          progress loop to terminate.
   * @param[in] startCallback                 user-defined callback function to be executed
   *                                          at the start of the progress thread.
   * @param[in] startCallbackArg              an argument to be passed to the start callback.
   * @param[in] delayedSubmissionCollection   collection of delayed submissions to be
   *                                          processed during progress.
   */
  static void progressUntilSync(
    ::ucxx::ProgressWorkerFunction progressFunction,
    PopulatePythonFuturesFunction populatePythonFuturesFunction,
    RequestNotifierFunction requestNotifierFunction,
    const bool& stop,
    ProgressThreadStartCallback startCallback,
    ProgressThreadStartCallbackArg startCallbackArg,
    std::shared_ptr<DelayedSubmissionCollection> delayedSubmissionCollection);

  void launchThread() override;

 public:
  WorkerProgressThread() = delete;

  /**
   * @brief Constructor of `ucxx::python::WorkerProgressThread`.
   *
   * The constructor for a `ucxx::python::WorkerProgressThread` object.
   *
   * WARNING: This is only intended for internal use of `ucxx::python::Worker`.
   *
   * @param[in] pollingMode                   whether the thread should use polling mode to
   *                                          progress.
   * @param[in] progressFunction              user-defined progress function implementation.
   * @param[in] populatePythonFuturesFunction user-defined function implementation to
   *                                          populate a Python future pool.
   * @param[in] requestNotifierFunction       user-defined function implementation to
   *                                          handle request notification.
   * @param[in] signalWorkerFunction          user-defined function to wake the worker
   *                                          progress event (when `pollingMode` is `false`).
   * @param[in] startCallback                 user-defined callback function to be executed
   *                                          at the start of the progress thread.
   * @param[in] startCallbackArg              an argument to be passed to the start callback.
   * @param[in] delayedSubmissionCollection   collection of delayed submissions to be
   *                                          processed during progress.
   */
  WorkerProgressThread(const bool pollingMode,
                       ::ucxx::ProgressWorkerFunction progressFunction,
                       PopulatePythonFuturesFunction populatePythonFuturesFunction,
                       RequestNotifierFunction requestNotifierFunction,
                       ::ucxx::SignalWorkerFunction signalWorkerFunction,
                       ProgressThreadStartCallback startCallback,
                       ProgressThreadStartCallbackArg startCallbackArg,
                       std::shared_ptr<DelayedSubmissionCollection> delayedSubmissionCollection);
};

}  // namespace python

}  // namespace ucxx
