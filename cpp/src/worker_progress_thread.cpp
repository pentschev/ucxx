/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <memory>

#include <ucxx/log.h>
#include <ucxx/worker_progress_thread.h>

namespace ucxx {

void WorkerProgressThread::progressUntilSync(
  std::function<bool(void)> progressFunction,
  std::function<void(void)> populatePythonFuturesFunction,
  std::function<void(void)> requestNotifierFunction,
  const bool& stop,
  ProgressThreadStartCallback startCallback,
  ProgressThreadStartCallbackArg startCallbackArg,
  std::shared_ptr<DelayedSubmissionCollection> delayedSubmissionCollection)
{
  if (startCallback) startCallback(startCallbackArg);

  while (!stop) {
    populatePythonFuturesFunction();

    if (delayedSubmissionCollection != nullptr) delayedSubmissionCollection->process();

    progressFunction();

    requestNotifierFunction();
  }
}

WorkerProgressThread::WorkerProgressThread(
  const bool pollingMode,
  std::function<bool(void)> progressFunction,
  std::function<void(void)> populatePythonFuturesFunction,
  std::function<void(void)> requestNotifierFunction,
  std::function<void(void)> signalWorkerFunction,
  ProgressThreadStartCallback startCallback,
  ProgressThreadStartCallbackArg startCallbackArg,
  std::shared_ptr<DelayedSubmissionCollection> delayedSubmissionCollection)
  : _pollingMode(pollingMode),
    _signalWorkerFunction(signalWorkerFunction),
    _startCallback(startCallback),
    _startCallbackArg(startCallbackArg)
{
  _thread = std::thread(WorkerProgressThread::progressUntilSync,
                        progressFunction,
                        populatePythonFuturesFunction,
                        requestNotifierFunction,
                        std::ref(_stop),
                        _startCallback,
                        _startCallbackArg,
                        delayedSubmissionCollection);
}

WorkerProgressThread::~WorkerProgressThread()
{
  if (!_thread.joinable()) {
    ucxx_warn("Worker progress thread not running or already stopped");
    return;
  }

  _stop = true;
  _signalWorkerFunction();
  _thread.join();
}

bool WorkerProgressThread::pollingMode() const { return _pollingMode; }

}  // namespace ucxx
