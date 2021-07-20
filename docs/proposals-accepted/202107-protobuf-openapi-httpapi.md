| type     | title                                                        | status   | menu               |
| -------- | ------------------------------------------------------------ | -------- | ------------------ |
| proposal | Expose Thanos APIs to OpenAPI/protobuf and expose on website and UIs | accepted | proposals-accepted |



## **Expose Thanos APIs to OpenAPI/protobuf and expose on website and UIs**

- Owners:
  - [@Hangzhi](https://github.com/Hangzhi)
- Related Tickets:
  - Program Proposal: https://github.com/thanos-io/thanos/issues/4102
  - Need for auto-documentation in Prometheus:
    - https://github.com/prometheus/prometheus/issues/7192
    - https://github.com/prometheus/prometheus/issues/5567
- Other docs:
  - A proposal of LFX mentorship: https://docs.google.com/document/d/1GeyFCk4QkVSrx4pQ4OSmOA1KE_FvmJRi_aq9hr2GoZQ/edit?usp=sharing

This design doc is proposing a design for Thanos HTTP API defined in protobuf and OpenAPI.

## **Motivations**

In order to improve Thanos usage for users, we would like to define our HTTP APIs in protobuf/OpenAPI and expose those in the website. This would allow users to use tools for documentation, validation, type checking, and even code generation to use our APIs efficiently.

### **Pitfalls of the current solution**

- Documentation needs to be written manually.

## **Goals**

- Define all APIs in protobuf.
- Be able to generate OpenAPI3 from protobuf.
- Auto-generate documentation with OpenAPI3 specification.
- Generate server code from API specification (OpenAPI3 or protobuf).
- Define all configuration potentially in protobuf too: https://github.com/openproto/protoconfig.
- Optionally we would like to have them on the index page on every Thanos component server.

## **Non-Goals**

- Not define Thanos gRPC APIs in protobuf specification.

## **How**

- Define Thanos APIs in protobuf specification.
- Generate OpenAPI from protobuf with gnostic extension.
- Generate documentation from OpenAPI with [swagger](https://github.com/swagger-api/swagger-codegen).
- Generate server API client and server stubs potentially from OpenAPI with [swagger](https://github.com/swagger-api/swagger-codegen) or [oapi-codegen](https://github.com/deepmap/oapi-codegen).

## **Alternatives**

## **Only define HTTP RESTful API only in OpenAPI. Not in protobuf.**

1. Pros:
   1. We are not sure whether we can definitely generate OpenAPI specifications from protobuf.
2. Cons:
   1. We write API definitions in protobuf for consistency.
   2. Protobuf definition is more concise.

## **Define APIs in gRPC and have RESTful APIs alongside with [grpc - gateway](https://github.com/grpc-ecosystem/grpc-gateway)**

1. Pros:
   1. We can have gRPC and RESTful APIs at the same time.
   2. We have gRPC APIs like rules API.
2. Cons:
   1. We don't know clearly how grpc-gateway works.
   2. We need to redefine our API service in gPRC since most of Thanos APIs and Promethues APIs are RESTful API.

## **Action Plan**

- [ ]  Experiments in rules API.
- [ ]  Define all http APIs in Protobuf.
- [ ]  Generate documents, client code, and server stubs from OpenAPI specification.

