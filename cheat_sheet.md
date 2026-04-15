# storage stats

```rql
from system::metrics::storage::table
map {
  id,
  tier,
  current_count,
  current_key: text::format_bytes(current_key_bytes),
  current_value: text::format_bytes(current_value_bytes),
  historical_count,
  historical_key: text::format_bytes(historical_key_bytes),
  historical_value: text::format_bytes(historical_value_bytes)
};
```

# cdc stats

```rql
from system::metrics::cdc::table
map {
  id,
  count,
  key: text::format_bytes(key_bytes),
  value: text::format_bytes(value_bytes)
};
```
