# erlkafka

```erlkafka``` is a kafka client written in erlang.

```erlkafka``` defines a simple API that provides seven core functions


* `produce/4` - native kafka produce request
* `multi_produce/2` - native kafka multi_produce request
* `fetch/4` - native kafka fetch request
* `multi_fetch/2` - native kafka multi_fetch request
* `offset/5` - native kafka offset request
* `get_list_of_brokers/0` - conditional zookeeper dependent list of brokers
* `get_list_of_broker_partitions/1` - conditional zookeeper dependent list of broker partitions for a topic

These functions can be found in the ```erlkafka_simple_api``` module [here](src/erlkafka_simple_api.erl)

# License

```erlkafka``` is available under two different licenses. LGPL or the BSD license.

erlkafka current verion : 0.6.1

Auto-discovery of brokers for Kafka is currently done via Zookeeper.  If you have the client set for `{enable_kafka_autodiscovery, true}` then is ezk (https://github.com/infinipool/ezk.git).

Setting this is currently done in `erlkafka.app.src`

```
  {env, [
      {enable_kafka_autodiscovery, true},
      {kafka_brokers, [{0, '127.0.0.1', 9092}]},
      {kafka_prefix, ""}
  ]}
```