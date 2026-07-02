# Per-Request Attribute Capture Design

## Goal

Move request-attribute capture from worker-wide configuration to an opt-in setting on each
request builder. This permits requests created on the same worker to choose independently
whether UCX request attributes are captured.

## Public API

`RequestBuilderBase` exposes `requestAttributes(bool enable = true)` for both lvalue and rvalue
builders. The setting defaults to `false`, matching the current default behavior.

`WorkerBuilder::requestAttributes()` and `Worker::isRequestAttributesEnabled()` are removed.
The worker no longer stores request-attribute configuration.

The deprecated request factories and convenience methods retain their existing signatures and
behavior. Requests created through those APIs do not capture request attributes. No new option
is added to a deprecated API.

## Construction and Data Flow

Each request stores its own `enableRequestAttributes` value. Request builders pass their setting
through an internal construction path before submission can occur. Existing deprecated public
factories use the same underlying construction logic with the value fixed to `false`.

`Request::publishRequest()` captures UCX attributes only when the request's setting is enabled.
`Request::queryAttributes()` throws `UnsupportedError` when capture was disabled for that request.
When capture was enabled but UCX did not provide a queryable request, it continues to throw
`NoElemError`.

A post-construction setter is not used because delayed submission may be configured to execute
immediately. The setting must be present before any request can publish its UCP handle.

## Active-Message Receive Timing

Active-message receive requests may be created internally when a message arrives before an
application requests it. A later builder call cannot retroactively capture attributes after UCX
has completed and released the underlying operation. Such a request reports attributes as
unavailable, consistent with other completion paths that have no queryable UCP request.

Internally created active-message receives, including callback-driven receives, default to
attribute capture disabled because no request builder selected an opt-in setting.

## Documentation and Errors

Request documentation and `UnsupportedError` text direct users to
`RequestBuilderBase::requestAttributes(true)` rather than worker construction. Builder examples
document the optional setting where appropriate.

## Testing

Tests cover:

- the absence of request-attribute configuration and state on workers;
- `requestAttributes()` fluent return types for lvalue and rvalue request builders;
- disabled-by-default behavior for builder-created requests;
- independent enabled and disabled requests on the same worker;
- successful capture for representative tag, stream, active-message, and memory requests when
  UCX produces a queryable request;
- unchanged disabled behavior for deprecated request creation paths; and
- existing `NoElemError` behavior for inline or otherwise non-queryable operations.

Focused request-builder and request tests are run first, followed by the complete C++ test suite
available in the local build.
