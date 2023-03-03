import json
from kafka import KafkaProducer
import time
import os

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


# w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))


# ethtx_config = EthTxConfig(
#         mongo_connection_string="mongodb://localhost/ethtx",  ##MongoDB connection string,
#         etherscan_api_key="UDC782U5N41BXTTT4DQ9ZBCSQV68VVTWDY",  ##Etherscan API key,
#         web3nodes={
#             "mainnet": {
#                 "hook": "https://sly-chaotic-shape.discover.quiknode.pro/",  # multiple nodes supported, separate them with comma || http://localhost:8545]
#                 # "hook": "http://localhost:8545",  

#                 "poa": False  # represented by bool value
#             }
#         },
#         default_chain="mainnet",
#         etherscan_urls={"mainnet": "https://api.etherscan.io/api", },
#     )

# ethtx = EthTx.initialize(ethtx_config)
# web3provider = ethtx.providers.web3provider




def init():
    global w3
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
    # w3 = Web3(Web3.HTTPProvider('https://sly-chaotic-shape.discover.quiknode.pro/'))
    print(w3.eth.get_block_number())

def usage(argv):
    print("python3 %s uid" % argv[0])   

def main(argv):
    if len(argv) < 2:
        usage(argv)
        exit()
    else :
        uid = argv[1]

    block_list = [] 
    transaction_list = []
    log_list = []
    block_check_start = None
    block_check_end = None
    counter = 0
    icounter = 0
    first_number = None
    pre_number = None
    producer = KafkaProducer(bootstrap_servers="localhost:9092",
         value_serializer=lambda m: json.dumps(m).encode()) 

    # send job to consumer
    send_work_topic_block = f'test_{uid}_block'.format(uid = uid)
    send_work_topic_transaction = f'test_{uid}_transaction'.format(uid = uid)
    send_work_topic_log = f'test_{uid}_log'.format(uid = uid)

    # send message to consumer_leader
    sync_message_topic = f'transaction_test_{uid}_leader'.format(uid = uid)

    JOB_SIZE = 10
    JOB_NUMBER = 5
    BATCH_SIZE = JOB_SIZE * JOB_NUMBER
    while (True) :
        cur_number = w3.eth.get_block_number()
        if first_number == None:
            first_number = cur_number
        last_number = cur_number
        if pre_number == None:
            # 1:7009160 2:7009210 3:7009260 4:7009310 5:7009360
            cur_number = 7009360 
            first_number = cur_number
            icounter +=1
            counter += 1
            pre_number = cur_number
            print("cur = %d last = %d"%(cur_number,last_number))
            print(cur_number)
            block_list.append(cur_number)
            transaction_list.append(cur_number)
            log_list.append(cur_number)
            block_check_start = time.time()
        elif pre_number < cur_number:
            
            icounter +=1
            counter +=1
            block_check_end = time.time()
            cur_number = pre_number+1
            pre_number = cur_number
            # print("check block cost %.2f"%(block_check_end - block_check_start))
            print("cur = %d last = %d"%(cur_number,last_number))
            # block_list.append(cur_number)
            if cur_number == 7009313:
                continue
            transaction_list.append(cur_number)
            # log_list.append(cur_number)
            block_check_start = time.time()
        if icounter >= JOB_SIZE:
            icounter = 0
            # producer.send(send_work_topic_block, block_list)
            producer.send(send_work_topic_transaction, transaction_list)
            # producer.send(send_work_topic_log, log_list)
            print("send topic")
            print(send_work_topic_block,send_work_topic_transaction,send_work_topic_log)
            print(block_list,transaction_list,log_list)
            
            block_list.clear()
            transaction_list.clear()
            log_list.clear()
            
            print("--------------\n")
            
        time.sleep(0.5)
        if counter  >= BATCH_SIZE:
            print("%d -> %d"%(first_number,cur_number))
            
            # 6740270 -> 6740397  
            sync_message = {}
            sync_message["type"] = "sync"
            sync_message["start"] = first_number
            sync_message["end"] = cur_number
            producer.send(sync_message_topic,sync_message)

            print(sync_message)
            print(sync_message_topic)
            break
if __name__=="__main__":
    
    init()
    main(sys.argv)

   