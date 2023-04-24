/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <ucxx/log.h>

#include <Python.h>

namespace ucxx {

namespace python {

PyObject* create_future_str        = NULL;
PyObject* call_soon_threadsafe_str = NULL;
PyObject* set_result_str           = NULL;
PyObject* set_exception_str        = NULL;

static int intern_strings(void)
{
  call_soon_threadsafe_str = PyUnicode_InternFromString("call_soon_threadsafe");
  if (call_soon_threadsafe_str == NULL) { return -1; }
  create_future_str = PyUnicode_InternFromString("create_future");
  if (create_future_str == NULL) { return -1; }
  set_exception_str = PyUnicode_InternFromString("set_exception");
  if (set_exception_str == NULL) { return -1; }
  set_result_str = PyUnicode_InternFromString("set_result");
  if (set_result_str == NULL) { return -1; }
  return 0;
}

static int init_ucxx_python()
{
  if (intern_strings() < 0) goto err;

  return 0;

err:
  if (!PyErr_Occurred()) PyErr_SetString(PyExc_RuntimeError, "could not initialize  Python C-API.");
  return -1;
}

PyObject* create_python_future(PyObject* event_loop)
{
  PyObject* result = NULL;

  PyGILState_STATE state = PyGILState_Ensure();

  if (init_ucxx_python() < 0) {
    if (!PyErr_Occurred()) PyErr_SetString(PyExc_RuntimeError, "could not allocate internals.");
    goto finish;
  }

  result = PyObject_CallMethodObjArgs(event_loop, create_future_str, NULL);
  if (PyErr_Occurred()) {
    ucxx_trace_req("Error calling event loop `create_future`.");
    PyErr_Print();
  }

finish:
  PyGILState_Release(state);
  return result;
}

PyObject* future_set_result(PyObject* event_loop, PyObject* future, PyObject* value)
{
  PyObject* result              = NULL;
  PyObject* set_result_callable = NULL;

  PyGILState_STATE state = PyGILState_Ensure();

  if (init_ucxx_python() < 0) {
    if (!PyErr_Occurred()) PyErr_SetString(PyExc_RuntimeError, "could not allocate internals.");
    goto finish;
  }

  set_result_callable = PyObject_GetAttr(future, set_result_str);
  if (PyErr_Occurred()) {
    ucxx_trace_req("Error getting future `set_result` method.");
    PyErr_Print();
  }
  if (!PyCallable_Check(set_result_callable)) {
    PyErr_Format(PyExc_RuntimeError,
                 "%s.%s is not callable.",
                 PyUnicode_1BYTE_DATA(future),
                 PyUnicode_1BYTE_DATA(set_result_str));
    goto finish;
  }

  result = PyObject_CallMethodObjArgs(
    event_loop, call_soon_threadsafe_str, set_result_callable, value, NULL);
  if (PyErr_Occurred()) {
    ucxx_trace_req("Error calling `call_soon_threadsafe` to set future result.");
    PyErr_Print();
  }

finish:
  Py_XDECREF(set_result_callable);
  PyGILState_Release(state);
  return result;
}

PyObject* future_set_exception(PyObject* event_loop,
                               PyObject* future,
                               PyObject* exception,
                               const char* message)
{
  PyObject* result                 = NULL;
  PyObject* set_exception_callable = NULL;
  PyObject* message_object         = NULL;
  PyObject* message_tuple          = NULL;
  PyObject* formed_exception       = NULL;

  PyGILState_STATE state = PyGILState_Ensure();

  if (init_ucxx_python() < 0) {
    if (!PyErr_Occurred()) PyErr_SetString(PyExc_RuntimeError, "could not allocate internals.");
    goto finish;
  }

  set_exception_callable = PyObject_GetAttr(future, set_exception_str);
  if (PyErr_Occurred()) {
    ucxx_trace_req("Error getting future `set_exception` method.");
    PyErr_Print();
  }
  if (!PyCallable_Check(set_exception_callable)) {
    PyErr_Format(PyExc_RuntimeError,
                 "%s.%s is not callable.",
                 PyUnicode_1BYTE_DATA(future),
                 PyUnicode_1BYTE_DATA(set_exception_str));
    goto finish;
  }

  message_object = PyUnicode_FromString(message);
  if (message_object == NULL) goto err;
  message_tuple = PyTuple_Pack(1, message_object);
  if (message_tuple == NULL) goto err;
  formed_exception = PyObject_Call(exception, message_tuple, NULL);
  if (formed_exception == NULL) goto err;

  result = PyObject_CallMethodObjArgs(
    event_loop, call_soon_threadsafe_str, set_exception_callable, formed_exception, NULL);
  if (PyErr_Occurred()) {
    ucxx_trace_req("Error calling `call_soon_threadsafe` to set future exception.");
    PyErr_Print();
  }
  goto finish;

err:
  PyErr_Format(PyExc_RuntimeError,
               "Error while forming exception for `asyncio.Future.set_exception`.");
finish:
  Py_XDECREF(message_object);
  Py_XDECREF(message_tuple);
  Py_XDECREF(formed_exception);
  Py_XDECREF(set_exception_callable);
  PyGILState_Release(state);
  return result;
}

}  // namespace python

}  // namespace ucxx
