# storage stats 

```rql
from system.table_storage_stats
map { 
  id, 
  current_count, 
  current_key: text::format_bytes(current_key_bytes), 
  current_value: text::format_bytes(current_value_bytes),
  historical_count, 
  historical_key: text::format_bytes(historical_key_bytes), 
  historical_value: text::format_bytes(historical_value_bytes),
  cdc_count, 
  cdc_key: text::format_bytes(cdc_key_bytes), 
  cdc_value: text::format_bytes(cdc_value_bytes)
};
```