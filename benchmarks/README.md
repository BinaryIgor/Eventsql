# Benchmarks

Various benchmarks to show performance of EventSQL.

## Queries
```
select id, convert_from(value, 'UTF8')::json from account_created_event limit 10;
create index account_created_event_email 
on account_created_event ((encode(value, 'escape')::json->>'email'));
```

## TODO
* sharding version tests -> endpoint to see when it's ready