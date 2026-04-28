# Agent Bus

Local event broker for the Agent Assistant system.

The production backend is Redis Streams. Use `memory://` only for tests and local
contract development.

Start Redis with Docker:

```bash
docker compose up -d redis
```

```bash
agent-bus --config config.yaml
```

Minimal config:

```yaml
server:
  host: 127.0.0.1
  port: 8790
redis_url: redis://127.0.0.1:6379/0
stream_prefix: agentbus
retention_maxlen: 10000
```
