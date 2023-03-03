import json
from kafka import KafkaProducer
import time
producer = KafkaProducer(bootstrap_servers="localhost:9092",
                        value_serializer=lambda m: json.dumps(m).encode())
result = {}
result["column1"] = 1
result["column2"] = 2
print(result)
producer.send("test_RingWave_1", dict(result))


# CREATE SOURCE IF NOT EXISTS source_test_RingWave_3 (
#             column1 integer,
#             column2 integer
#         )
#         WITH (
#             connector = 'kafka',
#             kafka.topic = 'test_RingWave_1',
#             kafka.brokers = 'message_queue:29092',
#             kafka.scan.startup.mode = 'latest'
#         )
#         ROW FORMAT JSON;


# create materialized view source_test_RingWave_3_view as
#         select
#             column1,
#             column2
#         from
#             source_test_RingWave_3;