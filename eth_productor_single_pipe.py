# 该程序的productor生产的数据带有type前缀，，方便后续consumer处理时适配对应的处理函数，较适用于离线批量生产追赶进度，batch设置一般较大
# 该Productor共用同个Kafka通道，Consumer主进程直接监听该Kafka通道
# 初始number通过while中的cur_number设置
import json
from kafka import KafkaProducer
import time

from ethtx import EthTx, EthTxConfig
from ethtx.models.decoded_model import DecodedTransaction, AddressInfo
from web3 import Web3
from ethtx.models.w3_model import W3Transaction, W3Block, W3Receipt, W3CallTree
from typing import List
from ethtx.models.decoded_model import (
    DecodedTransfer,
    DecodedBalance,
    DecodedEvent, DecodedCall,
)
from ethtx.models.objects_model import Transaction, Event, Block, Call
from ethtx.models.decoded_model import DecodedTransactionMetadata
import sys

import numpy as np
import time
from multiprocessing import Pool
import threading

from hexbytes import HexBytes



w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
start = time.time()
print(w3.eth.get_block_number())
end = time.time()
print(end - start)

producer = KafkaProducer(bootstrap_servers="localhost:9092",
         value_serializer=lambda m: json.dumps(m).encode())

pre_number = None
block_list = []
transaction_list = []
log_list = []

block_check_start = None
block_check_end = None
counter = 0

start_number = None
end_number = None

while (True) :
    cur_number = w3.eth.get_block_number()
    last_number = cur_number
    if pre_number == None:
        
        counter += 1
        cur_number = 7014600
        pre_number = cur_number
        print("last number = %d"%last_number)
        print(cur_number)
        start_number = cur_number

        block_list.append({"type":"block","val":cur_number})
        transaction_list.append({"type":"transaction","val":cur_number})
        log_list.append({"type":"log","val":cur_number})

    elif pre_number < cur_number:
        counter +=1
        block_check_end = time.time()
        cur_number = pre_number+1
        pre_number = cur_number
        # print("check block cost %.2f"%(block_check_end - block_check_start))
        print("cur = %d last = %d"%(cur_number,last_number))

        block_list.append({"type":"block","val":cur_number})
        transaction_list.append({"type":"transaction","val":cur_number})
        log_list.append({"type":"log","val":cur_number})

    time.sleep(0.2)
    if counter  >= 20:
        counter = 0
        end_number = cur_number

        producer.send("cx_single_test_20221201_1", block_list)
        producer.send("cx_single_test_20221201_1", transaction_list)
        producer.send("cx_single_test_20221201_1", log_list)

        block_list.clear()
        transaction_list.clear()
        log_list.clear()
        print("%d -----> %d"%(start_number,end_number))
        start_number = end_number+1



   

