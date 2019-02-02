# Запуск

```sh
beaver -c moses.ini -l moses.log -t navi
```

# Содержимое moses.ini

```sh
[beaver]
logstash_version: 1
mongo_connection_string: mongodb://logs.navi:27017
mongo_db: carrouting
mongo_batch_size: 10
format: raw
transport: navi
chunk_size: 1000000
number_of_consumer_processes: 4
max_queue_size: 50
bss_url: http://receiver.com/bss/3
bss_batch_size: 500
bss_storage_folder: /tmp/moses_bss
bss_ppnot_product: 36
bss_user_agent: moses

[/home/traffic/moses_post_data.log] ; paths are relative to your current path
type: logs
```
