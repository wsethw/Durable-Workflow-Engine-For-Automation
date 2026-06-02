# AetherFlow

AetherFlow is a durable workflow engine for automation, written in Go 1.22+. It stores workflow definitions and execution state in PostgreSQL, schedules work through Redis Streams, executes durable steps, exposes Prometheus metrics, and emits OpenTelemetry traces.

## Run

```sh
docker compose up --build
```

Services:

- API: http://localhost:8080
- Metrics: http://localhost:9090/metrics
- PostgreSQL 16: localhost:5432
- Redis 7: localhost:6379

## Endpoints

- `GET /healthz` returns liveness.
- `GET /readyz` checks PostgreSQL and Redis.
- `POST /definitions` creates a workflow definition.
- `POST /instances` starts an instance from a `definition_id`.
- `GET /instances/:id` returns current state, history, and payload.

API authentication is enabled when `API_KEYS` is set. Use comma-separated entries in the form
`token=tenant_id:role`, where role is `admin`, `operator`, or `reader`. When `API_KEYS` is empty,
the service runs in local development mode as `default/admin`.

Authorization is tenant-scoped. Definitions, instances, and execution history are read and written
through the tenant attached to the API key. Readers can inspect instances, operators can start
instances, and only admins can create definitions.

## Example

Create a definition:

```sh
curl -sS -X POST http://localhost:8080/definitions \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "name": "Order Processing Saga",
  "version": 1,
  "steps": [
    {
      "id": "reserve_inventory",
      "type": "http_request",
      "config": {
        "url": "https://httpbin.org/post",
        "method": "POST",
        "headers": {"Authorization": "Bearer {{.input.token}}"},
        "body": {"items": "{{.input.order.items}}"}
      },
      "retry": {"max_retries": 3, "initial_interval": "1s", "max_interval": "10s", "multiplier": 2.0},
      "on_failure": "release_inventory"
    },
    {
      "id": "process_payment",
      "type": "http_request",
      "config": {
        "url": "https://httpbin.org/post",
        "method": "POST",
        "body": {"amount": "{{.input.order.amount}}", "card_token": "{{.input.payment.token}}"}
      },
      "retry": {"max_retries": 2, "initial_interval": "500ms"},
      "on_failure": "reverse_payment"
    },
    {
      "id": "release_inventory",
      "type": "http_request",
      "config": {
        "url": "https://httpbin.org/post",
        "method": "POST",
        "body": {"reservation_id": "{{.steps.reserve_inventory.body.reservation_id}}"}
      }
    },
    {
      "id": "reverse_payment",
      "type": "http_request",
      "config": {
        "url": "https://httpbin.org/post",
        "method": "POST",
        "body": {"charge_id": "{{.steps.process_payment.body.charge_id}}"}
      }
    },
    {
      "id": "notify_customer",
      "type": "condition",
      "config": {
        "if": "steps.process_payment.status == 'completed'",
        "then": "send_email",
        "else": null
      }
    },
    {
      "id": "send_email",
      "type": "http_request",
      "config": {
        "url": "https://httpbin.org/post",
        "method": "POST",
        "body": {"to": "{{.input.order.customer_email}}", "template": "order_confirmation"}
      }
    }
  ]
}
JSON
```

Start an instance:

```sh
curl -sS -X POST http://localhost:8080/instances \
  -H 'Content-Type: application/json' \
  -d '{"definition_id":"<definition_id>","input":{"order":{"items":[{"sku":"abc","qty":1}],"amount":4200,"customer_email":"customer@example.com"},"payment":{"token":"tok_test"},"token":"demo-token"}}'
```

Fetch status:

```sh
curl -sS http://localhost:8080/instances/<instance_id>
```

## Runtime Guarantees

Each step is recorded as `running` before execution. Outputs and instance state are persisted before the next task is enqueued. On restart, workers consume the durable Redis stream and rehydrate instance state from PostgreSQL; the recovery loop also re-enqueues instances left in `running` or `waiting_timer`.

When a step is marked `running`, AetherFlow generates and persists `steps.<step_id>.idempotency_key`
in the execution environment. External systems should use that value as their idempotency token.
If a side effect succeeds but saving the final `completed` state fails, recovery retries the same
logical step with the same token.

Retries use per-step exponential backoff. A step with `on_failure` runs compensation steps in reverse completion order when the normal path cannot recover. Definition updates are versioned by `(name, version)`, and instances keep `definition_version`, so in-flight executions remain tied to their original definition snapshot.

Distributed workers claim instances through PostgreSQL leases before advancing state. Redis Stream messages are acknowledged only after a successful transition or a confirmed duplicate claim, and stale pending messages are reclaimed with `XAUTOCLAIM`. Durable timers are claimed atomically before enqueueing to avoid duplicate wakeups. HTTP workflow steps reject local/private destinations by default; set `ALLOW_PRIVATE_HTTP=true` only for trusted internal deployments.

HTTP requests validate the initial URL and every redirect target. Userinfo, non-HTTP schemes,
loopback, link-local, multicast, private, and locally resolved addresses are rejected unless
`ALLOW_PRIVATE_HTTP=true`.

## Fork/Join

`fork` and `join` are durable state-machine operations. A `fork` step declares branch start steps
and either a `join` step or a later `join` step in the definition. Branch steps are excluded from
the normal linear path, persisted in `state.forks`, and advanced one branch at a time by the engine.
The `join` step runs only after every branch in the fork has completed.

Use `config.next` to chain more than one step inside a branch:

```json
{
  "name": "Fanout Example",
  "version": 1,
  "steps": [
    {"id": "fanout", "type": "fork", "config": {"branches": ["reserve_a", "reserve_b"], "join": "join_reservations"}},
    {"id": "reserve_a", "type": "transform", "config": {"expr": "input.a", "next": "audit_a"}},
    {"id": "audit_a", "type": "transform", "config": {"expr": "steps.reserve_a.output.result"}},
    {"id": "reserve_b", "type": "transform", "config": {"expr": "input.b"}},
    {"id": "join_reservations", "type": "join", "config": {"next": "notify"}},
    {"id": "notify", "type": "transform", "config": {"expr": "'done'"}}
  ]
}
```

If a branch fails after retries, the fork state records the failed branch and the normal failure path
continues, including compensation when `on_failure` is configured.
