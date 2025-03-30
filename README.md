Simple and robust wrapper for producer/consumer based on Confluent.Kafka/librdkafka library

## TODO
### DLQ 

- [x] Basic version of consumer
- [x] Basic version of producer
- [x] Creating topics with administrator client
- [ ] Performance tests
- [ ] E2E testing using test containers
- [ ] DLQ/Retry support

### Outbox
- [ ] Row lock version


## Basic info
- Admin client creates topics with replication factor equals to brokers amount
- Each subscription from consumer creates main topic, N retries topics and DLQ topic (auto create is forbidden because consumer uses admin client)
    (producer is not allowed to create topics in any way)