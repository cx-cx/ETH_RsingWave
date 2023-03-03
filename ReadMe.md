# CX test
首先通过Yaml文件拉起所需要的容器
```sh
docker-compose up -d
```

发送数据，需要在eth环境下
进入eth环境
```sh
conda activate eth
```
---
1. 若使用生产者消费者模型 批处理模式：
拉起productor
```sh
python3 eth_productor_single_pipe.py
```
拉起consumer
```sh
python3 eth_consumer_single_pipe.py
```
---
2. 生产者消费者 实时处理
```sh
# 程序会先清空原有进程之后再重新拉起
# 若需要配置topic 需要修改文件中 cx_test_topic 改为你所需要的字段，程序根据该字段自动生成kafka_topic
# 想要从某一个block开始产生处理需要在eth_productor.py中进行设置
./bash run.sh
```
```sh
cd /home/ludev/work1/test_risingwave/risingwave-demo/Rs_neo4j/CryptoPlayground-master
python3 cx_test_pipline.py {uid}
```
---
3. 若不适用生成者消费者模型：
```sh
python3 produce_source.py all test multi
```
---
启动另一个终端
```sh
psql -h localhost -p 4566 -d dev -U root
```
**psql中大小写不敏感，需要使用""去特地标识为具有大写字符的字段**
**数据的链接核心在于topic的设置，需要设置produce_*.py和kafka.topic是同一个topic才能解析**
以某一种方式去消费、解析kafka中的数据
block表
```
CREATE MATERIALIZED SOURCE IF NOT EXISTS cx_test (
            difficulty bigint,
            "extraData" varchar,
            "gasLimit" bigint,
            "gasUsed" bigint,
            hash varchar,
            "logsBloom" varchar,
            miner varchar,
            "mixHash" varchar,
            nonce varchar,
            number bigint,
            "parentHash" varchar,
            "receiptsRoot" varchar,
            "sha3Uncles" varchar,
            size bigint,
            "stateRoot" varchar,
            timestamp bigint,
            "totalDifficulty" varchar,
            transactions text[],
            "transactionsRoot" varchar,
            uncles text[]
        )
        WITH (
            connector = 'kafka',
            kafka.topic = 'cx_test',
            kafka.brokers = 'message_queue:29092',
            kafka.scan.startup.mode = 'latest'
        )
        ROW FORMAT JSON;
```

构建基于元数据的数据视图
```
create materialized view cx_test_view as
        select
            difficulty,
            "extraData",
            "gasLimit",
            "gasUsed",
            hash,
            "logsBloom",
            miner,
            "mixHash",
            nonce,
            number,
            "parentHash",
            "receiptsRoot",
            "sha3Uncles",
            size,
            "stateRoot",
            timestamp,
            "totalDifficulty",
            transactions,
            "transactionsRoot",
            uncles
        from
            cx_test;
```

选取其中的某些字段或全部字段进行了解
```
select * from cx_test;
```

2. transaction表
```

CREATE MATERIALIZED SOURCE IF NOT EXISTS cx_test_transaction_2 (
            transaction_hash varchar, 
            from_address varchar, 
            to_address varchar, 
            timestamp bigint, 
            value varchar, 
            tx_fee bigint, 
            gas_limit bigint, 
            gas_price bigint, 
            gas_used bigint, 
            nonce bigint, 
            block_number bigint, 
            position bigint, 
            input varchar, 
            transaction_type bigint, 
            max_fee_per_gas bigint, 
            max_priority_fee_per_gas bigint
        )
        WITH (
            connector = 'kafka',
            kafka.topic = 'cx_test_transaction',
            kafka.brokers = 'message_queue:29092',
            kafka.scan.startup.mode = 'latest'
        )
        ROW FORMAT JSON;

create materialized view cx_test_transaction_view_2 as
        select
            transaction_hash, 
            from_address, 
            to_address, 
            timestamp, 
            value, 
            tx_fee, 
            gas_limit, 
            gas_price, 
            gas_used, 
            nonce, 
            block_number, 
            position, 
            input, 
            transaction_type, 
            max_fee_per_gas, 
            max_priority_fee_per_gas
        from
            cx_test_transaction_2;

select * from cx_test_transaction_view_2;
```
3. log表
```

CREATE MATERIALIZED SOURCE IF NOT EXISTS cx_test_log_3 (
            transaction_hash varchar, 
            log_index int, 
            call_id varchar, 
            event_signature varchar, 
            event_name varchar, 
            event_guessed boolean, 
            contract_address varchar, 
            contract_name varchar, 
            contract_badge varchar,
            parameter struct<name varchar,type varchar,value varchar >[]
        )
        WITH (
            connector = 'kafka',
            kafka.topic = 'cx_test_log',
            kafka.brokers = 'message_queue:29092',
            kafka.scan.startup.mode = 'latest'
        )
        ROW FORMAT JSON;

create materialized view cx_test_log_view_3 as
        select
            transaction_hash, 
            log_index, 
            call_id, 
            event_signature, 
            event_name, 
            event_guessed, 
            contract_address, 
            contract_name, 
            contract_badge,
            parameter
        from
            cx_test_log_3;

select * from cx_test_log_view_3;
```


