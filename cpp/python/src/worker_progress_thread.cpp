/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <memory>

#include <ucxx/python/worker_progress_thread.h>

namespace ucxx {

namespace python {

void WorkerProgressThread::progressUntilSync(
  ProgressWorkerFunction progressFunction,
  PopulatePythonFuturesFunction populatePythonFuturesFunction,
  RequestNotifierFunction requestNotifierFunction,
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
  ProgressWorkerFunction progressFunction,
  PopulatePythonFuturesFunction populatePythonFuturesFunction,
  RequestNotifierFunction requestNotifierFunction,
  SignalWorkerFunction signalWorkerFunction,
  ProgressThreadStartCallback startCallback,
  ProgressThreadStartCallbackArg startCallbackArg,
  std::shared_ptr<DelayedSubmissionCollection> delayedSubmissionCollection)
  : ::ucxx::WorkerProgressThread(pollingMode,
                                 progressFunction,
                                 signalWorkerFunction,
                                 startCallback,
                                 startCallbackArg,
                                 delayedSubmissionCollection),
    _populatePythonFuturesFunction(populatePythonFuturesFunction),
    _requestNotifierFunction(requestNotifierFunction)
{
  launchThread();
}

void WorkerProgressThread::launchThread()
{
  _thread = std::thread(WorkerProgressThread::progressUntilSync,
                        _progressFunction,
                        _populatePythonFuturesFunction,
                        _requestNotifierFunction,
                        std::ref(_stop),
                        _startCallback,
                        _startCallbackArg,
                        _delayedSubmissionCollection);
}

}  // namespace python

}  // namespace ucxx
