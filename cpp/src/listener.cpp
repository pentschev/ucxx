/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <memory>
#include <netinet/in.h>
#include <string>
#include <ucp/api/ucp.h>

#include <ucxx/exception.h>
#include <ucxx/listener.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>

namespace ucxx {

void ucpListenerDestructor(ucp_listener_h ptr)
{
  if (ptr != nullptr) ucp_listener_destroy(ptr);
}

Listener::Listener(std::shared_ptr<Worker> worker,
                   uint16_t port,
                   ucp_listener_conn_callback_t callback,
                   void* callbackArgs)
{
  if (worker == nullptr || worker->getHandle() == nullptr)
    throw ucxx::Error("Worker not initialized");

  ucp_listener_params_t params{};
  ucp_listener_attr_t attr{};

  params.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR | UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
  params.conn_handler.cb  = callback;
  params.conn_handler.arg = callbackArgs;

  if (ucxx::utils::sockaddr_set(&params.sockaddr, NULL, port))
    // throw std::bad_alloc("Failed allocation of sockaddr")
    throw std::bad_alloc();
  std::unique_ptr<ucs_sock_addr_t, void (*)(ucs_sock_addr_t*)> sockaddr(&params.sockaddr,
                                                                        ucxx::utils::sockaddr_free);

  ucp_listener_h handle = nullptr;
  utils::ucsErrorThrow(ucp_listener_create(worker->getHandle(), &params, &handle));
  _handle = std::unique_ptr<ucp_listener, void (*)(ucp_listener_h)>(handle, ucpListenerDestructor);
  ucxx_trace("Listener created: %p", _handle.get());

  attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
  utils::ucsErrorThrow(ucp_listener_query(_handle.get(), &attr));

  char ipString[INET6_ADDRSTRLEN];
  char portString[INET6_ADDRSTRLEN];
  ucxx::utils::sockaddr_get_ip_port_str(&attr.sockaddr, ipString, portString, INET6_ADDRSTRLEN);

  _ip   = std::string(ipString);
  _port = (uint16_t)atoi(portString);

  setParent(worker);
}

Listener::~Listener() { ucxx_debug("Listener destroyed: %p", _handle.get()); }

std::shared_ptr<Listener> createListener(std::shared_ptr<Worker> worker,
                                         uint16_t port,
                                         ucp_listener_conn_callback_t callback,
                                         void* callbackArgs)
{
  return std::shared_ptr<Listener>(new Listener(worker, port, callback, callbackArgs));
}

std::shared_ptr<Endpoint> Listener::createEndpointFromConnRequest(ucp_conn_request_h connRequest,
                                                                  bool endpointErrorHandling)
{
  auto listener = std::dynamic_pointer_cast<Listener>(shared_from_this());
  auto endpoint = ucxx::createEndpointFromConnRequest(listener, connRequest, endpointErrorHandling);
  return endpoint;
}

ucp_listener_h Listener::getHandle() { return _handle.get(); }

uint16_t Listener::getPort() { return _port; }

std::string Listener::getIp() { return _ip; }

}  // namespace ucxx
