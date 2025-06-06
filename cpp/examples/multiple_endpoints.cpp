/**
 * SPDX-FileCopyrightText: Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <ucxx/api.h>
#include <ucxx/utils/sockaddr.h>
#include <ucxx/utils/ucx.h>

static uint16_t listener_port = 12345;

class ListenerContext {
 private:
  std::shared_ptr<ucxx::Worker> _worker{nullptr};
  std::shared_ptr<ucxx::Endpoint> _endpoint{nullptr};
  std::shared_ptr<ucxx::Listener> _listener{nullptr};

 public:
  explicit ListenerContext(std::shared_ptr<ucxx::Worker> worker) : _worker{worker} {}

  void setListener(std::shared_ptr<ucxx::Listener> listener) { _listener = listener; }

  std::shared_ptr<ucxx::Listener> getListener() { return _listener; }

  std::shared_ptr<ucxx::Endpoint> getEndpoint() { return _endpoint; }

  void createEndpointFromConnRequest(ucp_conn_request_h conn_request)
  {
    static bool endpoint_error_handling = true;
    _endpoint = _listener->createEndpointFromConnRequest(conn_request, endpoint_error_handling);
  }
};

static void listener_cb(ucp_conn_request_h conn_request, void* arg)
{
  char ip_str[INET6_ADDRSTRLEN];
  char port_str[INET6_ADDRSTRLEN];
  ucp_conn_request_attr_t attr{};
  ListenerContext* listener_ctx = reinterpret_cast<ListenerContext*>(arg);

  attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
  ucxx::utils::ucsErrorThrow(ucp_conn_request_query(conn_request, &attr));
  ucxx::utils::sockaddr_get_ip_port_str(&attr.client_address, ip_str, port_str, INET6_ADDRSTRLEN);
  std::cout << "Server received a connection request from client at address " << ip_str << ":"
            << port_str << std::endl;

  listener_ctx->createEndpointFromConnRequest(conn_request);
}

enum class EndpointSource {
  Hostname = 0,
  WorkerAddress,
};

int main()
{
  // Create context with default feature flags
  auto context = ucxx::createContext({}, ucxx::Context::defaultFeatureFlags);

  // Create two workers - one for server, one for client
  auto server_worker = context->createWorker();
  auto client_worker = context->createWorker();

  // Create listener context and listener for server worker
  auto listener_ctx = std::make_unique<ListenerContext>(server_worker);
  auto listener     = server_worker->createListener(listener_port, listener_cb, listener_ctx.get());
  listener_ctx->setListener(listener);

  // Create 3 endpoints from client worker to server
  std::vector<std::shared_ptr<ucxx::Endpoint>> client_endpoints;
  std::cout << std::fixed
            << std::setprecision(3);  // Set floating point precision for timing output

  auto create_endpoints = [&](EndpointSource source) {
    for (int i = 0; i < 10; i++) {
      auto start_time = std::chrono::high_resolution_clock::now();

      std::shared_ptr<ucxx::Endpoint> endpoint;
      if (source == EndpointSource::Hostname) {
        endpoint = client_worker->createEndpointFromHostname("127.0.0.1", listener_port, true);
      } else {
        endpoint =
          client_worker->createEndpointFromWorkerAddress(server_worker->getAddress(), true);
      }
      client_endpoints.push_back(endpoint);

      auto end_time = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration<double, std::milli>(end_time - start_time);

      auto source_str = source == EndpointSource::Hostname ? "Hostname" : "WorkerAddress";

      std::cout << "Created client endpoint " << i + 1 << " from " << source_str
                << " with handle: " << std::hex
                << reinterpret_cast<uintptr_t>(endpoint->getHandle()) << std::dec << " (took "
                << duration.count() << " ms)" << std::endl;
    }
  };

  create_endpoints(EndpointSource::WorkerAddress);
  create_endpoints(EndpointSource::Hostname);

  // Progress both workers to ensure connections are established
  auto progress_start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 100; i++) {
    server_worker->progress();
    client_worker->progress();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto progress_end      = std::chrono::high_resolution_clock::now();
  auto progress_duration = std::chrono::duration<double, std::milli>(progress_end - progress_start);
  std::cout << "Total progress time: " << progress_duration.count() << " ms" << std::endl;

  return 0;
}
