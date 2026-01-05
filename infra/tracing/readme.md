# ReifyDB OpenTelemetry Tracing Infrastructure

This directory contains the Docker Compose setup for local tracing visualization using Grafana Tempo.

## Architecture

```
ReifyDB Application
    |
    | OTLP/gRPC (port 4317)
    v
Grafana Tempo (storage + query)
    |
    | HTTP API (port 3200)
    v
Grafana (visualization)
```

**Components:**
- **Grafana Tempo**: OpenTelemetry-native trace storage and query engine
- **Grafana**: Visualization and exploration UI

## Quick Start

### 1. Start the Tracing Stack

```bash
cd infra/tracing
docker-compose up -d
```

Verify services are running:
```bash
docker-compose ps
```

You should see:
- `reifydb-tempo`: healthy
- `reifydb-grafana`: healthy

### 2. Configure ReifyDB to Send Traces

#### Option A: Using `testcontainer` Binary

Edit `bin/testcontainer/src/main.rs` and uncomment the OTEL configuration (lines 28-35):

First, add the imports at the top of the file:
```rust
use reifydb::sub_server_otel::OtelConfig;
use std::time::Duration;
```

Then uncomment the `.with_tracing_otel()` call:
```rust
let mut db = server::memory()
    .await
    .unwrap()
    .with_http(http_config)
    .with_ws(ws_config)
    .with_tracing_otel(
        OtelConfig::new()
            .service_name("testcontainer")
            .endpoint("http://localhost:4317")
            .sample_ratio(1.0)
            .scheduled_delay(Duration::from_millis(500)),
        |t| t.with_filter("trace"),
    )
    .with_flow(|flow| flow)
    .build()
    .await
    .unwrap();
```

Then run:
```bash
cargo run --bin testcontainer
```

#### Option B: Using `server` Binary

Edit `bin/server/src/main.rs`:

Add imports at the top:
```rust
use reifydb::sub_server_otel::OtelConfig;
use std::time::Duration;
```

Add `.with_tracing_otel()` call after `.with_admin()` in the builder chain:
```rust
let mut db = server::memory()
    .await
    .unwrap()
    .with_http(HttpConfig::default().bind_addr("0.0.0.0:8090"))
    .with_ws(WsConfig::default().bind_addr("0.0.0.0:8091"))
    .with_admin(AdminConfig::default().bind_addr("127.0.0.1:9092"))
    .with_tracing_otel(
        OtelConfig::new()
            .service_name("reifydb-server")
            .endpoint("http://localhost:4317")
            .sample_ratio(1.0),
        tracing_configuration
    )
    .build()
    .await
    .unwrap();
```

#### Option C: Environment-Based Configuration

Create a wrapper script that reads from environment:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_SERVICE_NAME=reifydb
export OTEL_TRACES_SAMPLER=traceidratio
export OTEL_TRACES_SAMPLER_ARG=1.0

cargo run --bin testcontainer
```

### 3. Access Grafana

Open your browser to: **http://localhost:3000**

- No login required (anonymous auth enabled for dev)
- Navigate to **Explore** → Select **Tempo** datasource
- Choose **Search** tab to find traces
- Filter by:
  - Service Name: `testcontainer` or `reifydb-server`
  - Min/Max Duration
  - Status (OK, Error)

### 4. Querying Traces

#### Using Search UI
1. Go to Explore → Tempo
2. Use the **Search** tab
3. Select service name from dropdown
4. Add filters (duration, status, span name)
5. Click **Run Query**

#### Using TraceQL (Advanced)
```traceql
# Find all traces from testcontainer service
{ service.name = "testcontainer" }

# Find slow traces (>100ms)
{ duration > 100ms }

# Find errors
{ status = error }

# Find specific span names
{ name = "database_query" }

# Complex query
{ service.name = "testcontainer" && duration > 50ms && status = error }
```

## Configuration

### Sampling Rates

Control trace volume with `sample_ratio`:

- `1.0`: Trace everything (100%) - recommended for development
- `0.1`: Trace 10% of requests - good for production with high traffic
- `0.01`: Trace 1% of requests - production with very high traffic

Update in your ReifyDB configuration:
```rust
OtelConfig::new()
    .sample_ratio(0.1)  // 10% sampling
```

### Retention Policy

Default: **7 days**

To change, edit `infra/tracing/tempo/tempo.yaml`:
```yaml
compactor:
  compaction:
    block_retention: 336h  # 14 days
```

Then restart Tempo:
```bash
docker-compose restart tempo
```

### Storage

**Current**: Local filesystem in Docker volume `tempo-data`

**Production**: Migrate to object storage
- AWS S3
- Google Cloud Storage
- Azure Blob Storage
- MinIO (self-hosted S3-compatible)

Example S3 configuration (for production):
```yaml
storage:
  trace:
    backend: s3
    s3:
      bucket: reifydb-traces
      endpoint: s3.amazonaws.com
      access_key: ${AWS_ACCESS_KEY_ID}
      secret_key: ${AWS_SECRET_ACCESS_KEY}
```

## Troubleshooting

### No traces appearing in Grafana

1. **Check Tempo is receiving traces:**
   ```bash
   docker-compose logs tempo | grep -i otlp
   ```
   Look for: `msg="OTLP receiver started"`

2. **Verify ReifyDB is sending traces:**
   Check ReifyDB logs for:
   ```
   OpenTelemetry subsystem started service=testcontainer endpoint=http://localhost:4317
   ```

3. **Test connectivity:**
   ```bash
   docker-compose exec tempo wget -O- http://tempo:3200/ready
   ```

4. **Check for errors:**
   ```bash
   docker-compose logs tempo --tail=50
   docker-compose logs grafana --tail=50
   ```

### Tempo connectivity errors from ReifyDB

If ReifyDB runs on host (not in Docker), ensure Tempo is accessible:

```bash
# Test from host
curl http://localhost:4317
```

If connection refused, verify port mapping in `docker-compose.yml`.

### High memory usage

Tempo stores recent traces in memory. Reduce:

```yaml
# In tempo.yaml
ingester:
  max_block_duration: 2m  # Flush more frequently
  max_block_bytes: 500_000  # Smaller blocks
```

### Grafana datasource not auto-configured

1. Check provisioning directory is mounted:
   ```bash
   docker-compose exec grafana ls /etc/grafana/provisioning/datasources/
   ```

2. Manually add datasource:
   - Go to Configuration → Data Sources
   - Add data source → Tempo
   - URL: `http://tempo:3200`
   - Save & Test

## Advanced Usage

### Correlating Traces with Logs

Future enhancement: Add Loki for log aggregation.

```yaml
# Add to docker-compose.yml
loki:
  image: grafana/loki:latest
  ports:
    - "3100:3100"
  # ... configuration
```

Then update `grafana/provisioning/datasources/tempo.yaml`:
```yaml
jsonData:
  tracesToLogsV2:
    datasourceUid: loki
```

### Metrics from Traces

Enable metrics generator in `tempo.yaml`:
```yaml
metrics_generator:
  processor:
    service_graphs:
      enabled: true
    span_metrics:
      enabled: true
```

Requires Prometheus to scrape metrics.

### Production Deployment

For production environments:

1. **Disable anonymous auth** in Grafana:
   ```yaml
   environment:
     - GF_AUTH_ANONYMOUS_ENABLED=false
     - GF_SECURITY_ADMIN_PASSWORD=${ADMIN_PASSWORD}
   ```

2. **Use object storage** for Tempo (see Storage section above)

3. **Add authentication** to Tempo endpoints (reverse proxy with auth)

4. **Scale components**:
   - Multiple Tempo ingesters
   - Query frontend replicas
   - Load balancer for OTLP receivers

5. **Monitoring**: Add Prometheus + Grafana dashboards for Tempo itself

## Useful Links

- [Grafana Tempo Documentation](https://grafana.com/docs/tempo/latest/)
- [TraceQL Reference](https://grafana.com/docs/tempo/latest/traceql/)
- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)
- [ReifyDB OTEL Subsystem](../../crates/sub-server-otel/)

## Maintenance

### Backup Tempo Data

```bash
docker-compose stop tempo
tar -czf tempo-backup-$(date +%Y%m%d).tar.gz -C /var/lib/docker/volumes tempo-data
docker-compose start tempo
```

### Clean Up Old Traces

Tempo automatically manages retention based on `block_retention` setting.

Manual cleanup:
```bash
docker-compose down
docker volume rm tracing_tempo-data
docker-compose up -d
```

### Update Images

```bash
docker-compose pull
docker-compose up -d
```

## Shutting Down

```bash
# Stop services (preserves data)
docker-compose stop

# Stop and remove containers (preserves volumes)
docker-compose down

# Stop and remove everything including volumes (DANGER: data loss)
docker-compose down -v
```
