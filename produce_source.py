# 该程序用于直接以blockNumber进行生产数据，便于后续内容的测试
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
    except:
        print("error!!!!!\n\n")
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



def create_block_num_arr(size):
    if size == "test":
        # print (w3.eth.block_number)
        return [7009160,7009161,7009162,7009163,7009164,7009165,7009166,7009167,7009168,7009169,
                7009170,7009171,7009172,7009173,7009174,7009175,7009176,7009177,7009178,7009179,
               ] # first_test
        # return [4500004,4500005] # second_test
    if size == "multi_test":
        # print (w3.eth.block_number)
        # return [6326520,6326521,6326522,6326523,6326524,6326525,6326526,6326527,6326528] # first_test
        # return [4500004,4500005] # second_test
        # return [14872768,14872769,14872770,14872771,14872772,14872773,14872774,14872775,14872776,14872777,14872778,14872779,14872780,14872781,14872782,14872783,14872784,14872785,14872786,14872787,14872788,14872789,14872790,14872791]
        # return [4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,]
        # return [4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,4500002,]
        return [6725381]
        # return [6725382]
        # return [6725379,6725380]
        # return [6725383,6725384,6725385,6725386]
    last_number = w3.eth.block_number
    np.random.seed(0)  
    block_num_arr = np.random.randint(1,int(last_number),size=int(size),dtype=int)
    return block_num_arr

def usage(argv):
    print("python3 %s type size multi/single" % argv[0])
    print("python3 %s all test multi" % argv[0])
    print("上面的命令会产生由于multi会走多进程处理则type入参无效可以随便填，数据的size为test会产生固定的20个block进行测试\n而单独测试的还未完成")

def block_produce(num):
    
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
    time_get_block_start = time.time()
    w3block: W3Block = web3provider.get_block(int(num))
    time_get_block_end = time.time()
    # print("get block cost time %.3f second" %(time_get_block_end - time_get_block_start))
    print(int(num))
    block: Block = Block.from_raw(
        w3block=w3block,
        chain_id="mainnet",
    )
    results = {}
    first = 0
    for tx in w3block.transactions:
        time_tr_start = time.time()
        tx = tx.hex()
        print(tx)
        if tx != "0xd692b8807bbd18e041fbbe5e7b738809377145fb6418f209331e3bcf8c64eb5a":
            continue
        first = tx
        result = {}
        
        w3txdetail = w3.eth.get_transaction(tx)
        w3txreceipt = w3.eth.get_transaction_receipt(tx)
        w3transaction: W3Transaction = web3provider.get_transaction(tx)
        w3receipt: W3Receipt = web3provider.get_receipt(tx)
        w3calls: W3CallTree = web3provider.get_calls(tx)
        time_raw_start = time.time()
        # read the raw transaction from the node
        transaction = Transaction.from_raw(w3transaction=w3transaction, w3receipt=w3receipt, w3calltree=w3calls)
        time_raw_end = time.time()
        print("get raw transaction cost time %.3f second" %(time_raw_end - time_raw_start))
        
        # get proxies used in the transaction
        time_get_proxies_start = time.time()
        proxies = ethtx.decoders.get_proxies(transaction.root_call, "mainnet")
        time_get_proxies_end = time.time()
        print("get proxies  cost time %.3f second" %(time_get_proxies_end - time_get_proxies_start))
        
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
        decoded_metadata: DecodedTransactionMetadata = (
            ethtx.decoders.semantic_decoder.decode_metadata(
                block.metadata, transaction.metadata, "mainnet"
            )
        )
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
        
        

            


def main(argv):
    if len(argv) < 4:
        usage(argv)
    else :
        block_num_arr = create_block_num_arr(argv[2])
        if argv[3] == "multi":
            pool = Pool(processes=20,initializer = init,initargs=('child',))
            # results_arr = pool.map_async(block_produce, block_num_arr)
            # for result in results_arr.get():
            #     print(result)
            #     producer.send("block_test_20230217_kafka", result)

            # results_arr = pool.map_async(log_produce, block_num_arr)
            # for results in results_arr.get():
            #     for events in results.values():
            #         for event in events:
            #             producer.send("log_test_20221123_1", event)
            #             print(event)

            results_arr = pool.map_async(transaction_produce, block_num_arr)
            for results in results_arr.get():
                for result in results.values():
                    producer.send("transaction_test_20230221_kafka", result)
                    print(result)

            
            
            pool.close()
            pool.join()
        else:
            if argv[1] == "block":
                for num in block_num_arr:
                    block_produce(num)
            elif argv[1] == "transaction":
                for num in block_num_arr:
                    transaction_produce(num)
            elif argv[1] == "log":
                s_time = time.time()
                for num in block_num_arr:
                    log_produce(num)
                e_time = time.time()
                # print("first run %.3f second" %(e_time - s_time))
                # print("second run %.3f second" %(e2_time - e_time)) 
producer = None
ethtx_config = None
ethtx = None
web3provider = None
w3 = None
MANGODB_ADDRESS = "mongodb://localhost/ethtx"
def init(init_type):
    global producer,ethtx_config,ethtx,web3provider,w3
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

if __name__=="__main__":
    init("main")
    main(sys.argv)
