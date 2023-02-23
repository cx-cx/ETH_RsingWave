from kafka import KafkaConsumer
from kafka import KafkaProducer
import os
import time
import json

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

import multiprocessing
from multiprocessing import Pool
import threading

from hexbytes import HexBytes



def analysis_params(param_list):
    analysis = []
    try:
        if param_list != None:
            for param in param_list:
                sub_analysis = {}
                sub_analysis['name'] = param.name
                sub_analysis['type'] = param.type
                if isinstance(param.value, AddressInfo):
                    sub_analysis['value'] = {'address':param.value.address, 'name':param.value.name, 'badge':param.value.badge}
                elif param.type == 'call': 
                    sub_analysis['value'] = {'contract':param.value['contract'].address, 'contract_name':param.value['contract'].name, 'contract_badge':param.value['contract'].badge, 'function_name':param.value['function_name'], 'argument':analysis_params(param.value['arguments'])}
                else: 
                    sub_analysis['value'] = param.value
                sub_analysis['value'] = str(sub_analysis['value'])
                
                analysis.append(sub_analysis)
    except Exception as err:
        print("error!!!!!")
        print(err)

        print(analysis)
    else :
        return analysis

def analysis_events(decoded_events,transaction_hash):
    analysis = []
    if decoded_events != None:
        if isinstance(decoded_events, list):
            for event in decoded_events:
                sub_analysis = {}
                sub_analysis['transaction_hash'] = transaction_hash
                sub_analysis['log_index'] = event.index
                sub_analysis['call_id'] = event.call_id
                sub_analysis['event_signature'] = event.event_signature
                sub_analysis['event_name'] = event.event_name
                sub_analysis['event_guessed'] = event.event_guessed
                sub_analysis['contract_address'] = event.contract.address
                sub_analysis['contract_name'] = event.contract.name
                sub_analysis['contract_badge'] = event.contract.badge
                sub_analysis['parameter'] = analysis_params(event.parameters)

                analysis.append(sub_analysis)
        else:
            sub_analysis = {}
            sub_analysis['transaction_hash'] = transaction_hash
            sub_analysis['log_index'] = decoded_events.index
            sub_analysis['call_id'] = decoded_events.call_id
            sub_analysis['event_signature'] = decoded_events.event_signature
            sub_analysis['event_name'] = decoded_events.event_name
            sub_analysis['event_guessed'] = decoded_events.event_guessed
            sub_analysis['contract_address'] = decoded_events.contract.address
            sub_analysis['contract_name'] = decoded_events.contract.name
            sub_analysis['contract_badge'] = decoded_events.contract.badge
            sub_analysis['parameter'] = analysis_params(decoded_events.parameters)
            
            analysis.append(sub_analysis)

    return analysis

def analysis_calls(decoded_calls):
    analysis = []
    if decoded_calls != None:
        if isinstance(decoded_calls, list):
            for call in decoded_calls:
                sub_analysis = {}
                sub_analysis['call_id'] = call.call_id
                sub_analysis['indent'] = call.indent
                sub_analysis['call_type'] = call.call_type
                sub_analysis['status'] = call.status
                sub_analysis['function_signature'] = call.function_signature
                sub_analysis['function_name'] = call.function_name
                sub_analysis['function_guessed'] = call.function_guessed
                sub_analysis['from_address'] = call.from_address.address
                sub_analysis['from_address_name'] = call.from_address.name
                sub_analysis['from_address_badge'] = call.from_address.badge
                sub_analysis['to_address'] = call.to_address.address
                sub_analysis['to_address_name'] = call.to_address.name
                sub_analysis['to_address_badge'] = call.to_address.badge
                sub_analysis['value'] = call.value
                sub_analysis['gas_used'] = call.gas_used

                if call.subcalls is not None:
                    sub_analysis['subcall'] = analysis_calls(call.subcalls)
                else:
                    sub_analysis['subcall'] = []

                analysis.append(sub_analysis)
        else:
            sub_analysis = {}
            sub_analysis['call_id'] = decoded_calls.call_id
            sub_analysis['indent'] = decoded_calls.indent
            sub_analysis['call_type'] = decoded_calls.call_type
            sub_analysis['status'] = decoded_calls.status
            sub_analysis['function_signature'] = decoded_calls.function_signature
            sub_analysis['function_name'] = decoded_calls.function_name
            sub_analysis['function_guessed'] = decoded_calls.function_guessed
            sub_analysis['from_address'] = decoded_calls.from_address.address
            sub_analysis['from_address_name'] = decoded_calls.from_address.name
            sub_analysis['from_address_badge'] = decoded_calls.from_address.badge
            sub_analysis['to_address'] = decoded_calls.to_address.address
            sub_analysis['to_address_name'] = decoded_calls.to_address.name
            sub_analysis['to_address_badge'] = decoded_calls.to_address.badge
            sub_analysis['value'] = decoded_calls.value
            sub_analysis['gas_used'] = decoded_calls.gas_used

            if decoded_calls.subcalls is not None:
                sub_analysis['subcall'] = analysis_calls(decoded_calls.subcalls)
            else:
                sub_analysis['subcall'] = []

            analysis.append(sub_analysis)

    return analysis

def analysis_transfers(decoded_transfers):
    analysis = []
    if decoded_transfers != None:
        if isinstance(decoded_transfers, list):
            for transfer in decoded_transfers:
                sub_analysis = {}
                sub_analysis['token_symbol'] = transfer.token_symbol
                sub_analysis['token_address'] = transfer.token_address
                sub_analysis['token_standard'] = transfer.token_standard
                sub_analysis['from_address'] = transfer.from_address.address
                sub_analysis['from_address_name'] = transfer.from_address.name
                sub_analysis['from_address_badge'] = transfer.from_address.badge
                sub_analysis['to_address'] = transfer.to_address.address
                sub_analysis['to_address_name'] = transfer.to_address.name
                sub_analysis['to_address_badge'] = transfer.to_address.badge
                sub_analysis['value'] = transfer.value

                analysis.append(sub_analysis)
        else:
            sub_analysis = {}
            sub_analysis['token_symbol'] = decoded_transfers.token_symbol
            sub_analysis['token_address'] = decoded_transfers.token_address
            sub_analysis['token_standard'] = decoded_transfers.token_standard
            sub_analysis['from_address'] = decoded_transfers.from_address.address
            sub_analysis['from_address_name'] = decoded_transfers.from_address.name
            sub_analysis['from_address_badge'] = decoded_transfers.from_address.badge
            sub_analysis['to_address'] = decoded_transfers.to_address.address
            sub_analysis['to_address_name'] = decoded_transfers.to_address.name
            sub_analysis['to_address_badge'] = decoded_transfers.to_address.badge
            sub_analysis['value'] = decoded_transfers.value
            
            analysis.append(sub_analysis)

    return analysis

def analysis_balances(decoded_balances):
    analysis = []
    if decoded_balances != None:
        if isinstance(decoded_balances, list):
            for balance in decoded_balances:
                sub_analysis = {}
                sub_analysis['holder'] = balance.holder.address
                sub_analysis['holder_name'] = balance.holder.name
                sub_analysis['holder_badge'] = balance.holder.badge
                sub_analysis['token_list'] = []
                if balance.tokens is not None:
                    for token in balance.tokens:
                        sub_analysis['token_list'].append({'symbol': token['token_symbol'], 'address': token['token_address'], 'standard': token['token_standard'], 'balance': token['balance']})

                analysis.append(sub_analysis)
        else:
            sub_analysis = {}
            sub_analysis['holder'] = decoded_balances.holder.address
            sub_analysis['holder_name'] = decoded_balances.holder.name
            sub_analysis['holder_badge'] = decoded_balances.holder.badge
            sub_analysis['token_list'] = []
            if decoded_balances.tokens is not None:
                for token in decoded_balances.tokens:
                    sub_analysis['token_list'].append({'symbol': token['token_symbol'], 'address': token['token_address'], 'standard': token['token_standard'], 'balance': token['balance']})   
                         
            analysis.append(sub_analysis)

    return analysis


class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        return super().default(obj)


def block_produce(num):
    print("%d processing block num = %s"%(os.getpid(),num))
    tx = w3.eth.get_block(int(num))

    # 对数据进行处理
    tx_dict = dict(tx)
    tx_json = json.dumps(tx_dict, cls=HexJsonEncoder) # 去除其中的Hexsh字段和序列化为格式正确的json
    tx_json_load = json.loads(tx_json)  # 反序列化为python对象
    tx_dict = dict(tx_json_load)    # 转化为字典对象
    tx_dict["totalDifficulty"] = str(tx_dict["totalDifficulty"])    # 将totalDifficulty字段更改为string类型避免数字过大无法识别

    # producer = KafkaProducer(bootstrap_servers="localhost:9092",
    #         value_serializer=lambda m: json.dumps(m).encode())

    return tx_dict

def transaction_produce(num):
    print("%d processing transaction num = %s"%(os.getpid(),num))

    w3block: W3Block = web3provider.get_block(int(num))

    block: Block = Block.from_raw(
        w3block=w3block,
        chain_id="mainnet",
    )
    results = {}
    first = 0
    for tx in w3block.transactions:
        tx = tx.hex()
        first = tx
        result = {}
        
        w3txdetail = w3.eth.get_transaction(tx)
        w3txreceipt = w3.eth.get_transaction_receipt(tx)
        w3transaction: W3Transaction = web3provider.get_transaction(tx)
        w3receipt: W3Receipt = web3provider.get_receipt(tx)

        # read the raw transaction from the node
        # get proxies used in the transaction
        
        # decode transaction components
        

        result = {}

        metadata = w3transaction.to_object(w3receipt)
        result['transaction_hash'] = tx
        result['from_address'] = metadata.from_address
        result['to_address'] = metadata.to_address
        result['timestamp'] = w3block.timestamp
        result['value'] = str(metadata.tx_value)
        result['tx_fee'] = metadata.gas_price * metadata.gas_used
        result['gas_limit'] = metadata.gas_limit
        result['gas_price'] = metadata.gas_price
        result['gas_used'] = metadata.gas_used    
        result['nonce'] = w3transaction.nonce
        result['block_number'] = w3transaction.blockNumber
        result['position'] = metadata.tx_index
        result['input'] = w3transaction.input
        result['transaction_type'] = int(w3txreceipt.type, 16)
        if result['transaction_type'] > 1:    
            result['max_fee_per_gas'] = w3txdetail.maxFeePerGas
            result['max_priority_fee_per_gas'] = w3txdetail.maxPriorityFeePerGas
        else:
            result['max_fee_per_gas'] = -1
            result['max_priority_fee_per_gas'] = -1

        results[tx] = result
    return results

def log_produce(num):
    print("%d processing log num = %s"%(os.getpid(),num))
    time_get_block_start = time.time()
    w3block: W3Block = web3provider.get_block(int(num))
    time_get_block_end = time.time()
    # print("get block cost time %.3f second" %(time_get_block_end - time_get_block_start))
    # print(int(num))
    block: Block = Block.from_raw(
        w3block=w3block,
        chain_id="mainnet",
    )
    results = {}
    first = 0
    for tx in w3block.transactions:
        # print("tx = "+str(tx.hex())+" will process")
        time_tr_start = time.time()
        tx = tx.hex()
        first = tx
        result = {}
        
        try:
            w3txdetail = w3.eth.get_transaction(tx)
            w3txreceipt = w3.eth.get_transaction_receipt(tx)
            w3transaction: W3Transaction = web3provider.get_transaction(tx)
            w3receipt: W3Receipt = web3provider.get_receipt(tx)
            w3calls: W3CallTree = web3provider.get_calls(tx)
        except Exception as err:
            print("get raw transaction error!!!!!")
            print(err)
            print(tx)
            continue




        time_raw_start = time.time()
        # read the raw transaction from the node
        transaction = Transaction.from_raw(w3transaction=w3transaction, w3receipt=w3receipt, w3calltree=w3calls)
        time_raw_end = time.time()
        # print("get raw transaction cost time %.3f second" %(time_raw_end - time_raw_start))
        
        # get proxies used in the transaction
        time_get_proxies_start = time.time()
        proxies = ethtx.decoders.get_proxies(transaction.root_call, "mainnet")
        time_get_proxies_end = time.time()
        # print("get proxies  cost time %.3f second" %(time_get_proxies_end - time_get_proxies_start))
        
        # decode transaction components
        time_deco_start = time.time()
        abi_decoded_events: List[Event] = ethtx.decoders.abi_decoder.decode_events(
            transaction.events, block.metadata, transaction.metadata
        )
        # print("decoded_event succeed")
        
        # abi_decoded_calls: DecodedCall = ethtx.decoders.abi_decoder.decode_calls(
        #     transaction.root_call, block.metadata, transaction.metadata, proxies
        # )
        # abi_decoded_transfers: List[
        #     DecodedTransfer
        # ] = ethtx.decoders.abi_decoder.decode_transfers(abi_decoded_calls, abi_decoded_events)
        # abi_decoded_balances: List[DecodedBalance] = ethtx.decoders.abi_decoder.decode_balances(
        #     abi_decoded_transfers
        # )
        time_deco_end = time.time()
        # print("decode transaction cost time %.3f second" %(time_deco_end - time_deco_start))


        # semantically decode transaction components
        time_semantical_start = time.time()
        

        try:
            decoded_metadata: DecodedTransactionMetadata = (
            ethtx.decoders.semantic_decoder.decode_metadata(
                    block.metadata, transaction.metadata, "mainnet"
                )
            )
        except Exception as err:
            print("decode error!!!!!")
            print(err)
            print(tx)
            continue
        # print("decoded_metadata succeed")

        decoded_events: List[DecodedEvent] = ethtx.decoders.semantic_decoder.decode_events(
            abi_decoded_events, decoded_metadata, proxies
        )
        # print("decoded_events_semantically succeed")

        # decoded_calls: Call = ethtx.decoders.semantic_decoder.decode_calls(
        #     abi_decoded_calls, decoded_metadata, proxies
        # )
        # decoded_transfers: List[
        #     DecodedTransfer
        # ] = ethtx.decoders.semantic_decoder.decode_transfers(
        #     abi_decoded_transfers, decoded_metadata
        # )
        # decoded_balances: List[
        #     DecodedBalance
        # ] = ethtx.decoders.semantic_decoder.decode_balances(
        #     abi_decoded_balances, decoded_metadata
        # )
        time_semantical_end = time.time()
        # print("semantical decode transaction cost time %.3f second" %(time_semantical_end - time_semantical_start))

        metadata = w3transaction.to_object(w3receipt)
        # for event in analysis_events(decoded_events,tx):
        #     time_send_start = time.time()
        #     producer.send(kafka_topic, event)
        #     time_send_end = time.time()
        #     print("****-------****")
        #     print(event)
        #     print("send events cost time %.3f second" %(time_send_end - time_send_start))
        #     print("****-------****")

            
        events_list = analysis_events(decoded_events,tx)
        time_tr_end = time.time()
        # print("transaction %s cost %.3f second" %(tx,time_tr_end - time_tr_start))
        results[tx] = events_list
    return results

# def process_main(elem) :
#     # print("pid %d process block %s"%(os.getpid(),elem["val"]))
#     if elem["type"] == "block":
#         time.sleep(1)
#         # return "block",elem["val"]
#         return block_produce(int(elem["val"]))
#     elif elem["type"] == "transaction" :
#         return transaction_produce(int(elem["val"]))
#     elif elem["type"] == "log" :
#         return log_produce(int(elem["val"]))
#     else :
#         return "error"





producer = None
ethtx_config = None
ethtx = None
web3provider = None
w3 = None
MANGODB_ADDRESS = "mongodb://localhost/ethtx"
consumer = None
def init(init_type,consumer_topic="test"):
    
    pid = os.getpid()
    print("pid = %d init"%pid)
    global producer,ethtx_config,ethtx,web3provider,w3,consumer
    if init_type == "main":
        producer = KafkaProducer(bootstrap_servers="localhost:9092",
            value_serializer=lambda m: json.dumps(m).encode())

    if init_type == "child" :
        ethtx_config = EthTxConfig(
            mongo_connection_string=MANGODB_ADDRESS,  ##MongoDB connection string,
            etherscan_api_key="UDC782U5N41BXTTT4DQ9ZBCSQV68VVTWDY",  ##Etherscan API key,
            web3nodes={
                "mainnet": {
                    # "hook": "https://sly-chaotic-shape.discover.quiknode.pro/",  # multiple nodes supported, separate them with comma || http://localhost:8545]
                    "hook": "http://localhost:8545",  

                    "poa": False  # represented by bool value
                }
            },
            default_chain="mainnet",
            etherscan_urls={"mainnet": "https://api.etherscan.io/api", },
        )
        ethtx = EthTx.initialize(ethtx_config)
        web3provider = ethtx.providers.web3provider
    # w3 = Web3(Web3.HTTPProvider('https://sly-chaotic-shape.discover.quiknode.pro/'))
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
    if consumer_topic != "test" :
        consumer = KafkaConsumer(consumer_topic,
                         bootstrap_servers="localhost:9092",
                         group_id=consumer_topic+"_2",
                         auto_offset_reset='earliest')

def kafka_clinet_process(process_type,kafka_topic,uid):
    init("main",kafka_topic)
    # pool = None
    if process_type == "block" :
        pool = Pool(processes=1,initializer = init,initargs=('child',))
        result_send_topic = f'block_test_{uid}'.format(uid = uid)
        process_main = block_produce
    elif process_type == "transaction":
        pool = Pool(processes=4,initializer = init,initargs=('child',))
        process_main = transaction_produce
        result_send_topic = f'transactio_test_{uid}'.format(uid = uid)
    elif process_type == "log":
        pool = Pool(processes=16,initializer = init,initargs=('child',))
        process_main = log_produce
        result_send_topic = f'log_test_{uid}'.format(uid = uid)
        
    else :
        print("error type ")
        exit()
    counter = 0 
    for msg in consumer:
        elems = json.loads(msg.value)
        print("will process follow elem process type %s batch = %d"%(process_type,counter))
        print(elems)
        counter+=1

        results_arr = pool.map_async(process_main, elems)
        start = time.time()
        for results in results_arr.get():
            if process_type == "block":
                producer.send(result_send_topic, results)
                # print(results)
            elif process_type == "transaction" :
                for result in results.values():
                    producer.send(result_send_topic, result)
                    # print(result)
            elif process_type == "log":
                for events in results.values():
                    for event in events:
                        producer.send(result_send_topic, event)
                        # print(event)
        end = time.time()
        print("type = %s send message cost time %.2f"%(process_type,end-start))
        print("send topic = %s"%(result_send_topic))
        # if (counter == 7):
        #     print("%s succeed"%process_type)
        #     break
    
    pool.close()
    pool.join()
    # print("%s pool close succeed"%process_type)
def main(argv):

    if len(argv) < 2:
        usage(argv)
        exit()
    else :
        uid = argv[1]
    get_work_topic_block = f'test_{uid}_block'.format(uid = uid)
    get_work_topic_transaction = f'test_{uid}_transaction'.format(uid = uid)
    get_work_topic_log = f'test_{uid}_log'.format(uid = uid)

    block_client = multiprocessing.Process(target = kafka_clinet_process,args = ("block",get_work_topic_block,uid))
    transaction_client = multiprocessing.Process(target = kafka_clinet_process,args = ("transaction",get_work_topic_transaction,uid))
    log_client = multiprocessing.Process(target = kafka_clinet_process,args = ("log",get_work_topic_log,uid))
    
    block_client.start()
    transaction_client.start()
    log_client.start()

    block_client.join()
    transaction_client.join()
    log_client.join()

    
def usage(argv):
    print("python3 %s uid" % argv[0])


if __name__=="__main__":
    
    init("main")
    main(sys.argv)
