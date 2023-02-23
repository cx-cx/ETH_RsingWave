# 该程序用于profile函数耗时问题
# pyinstrument  --outfile=test_block_1_7198400_second.html -r html --hide=EXPR test_new.py
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
    return "log",results

def create_arr(num,size):
    arr = []
    for i in range(0,size):
        arr.append(num+i)
    return arr


from multiprocessing import Pool
import random

producer = None
ethtx_config = None
ethtx = None
web3provider = None
w3 = None
MANGODB_ADDRESS = "mongodb://localhost/ethtx"
consumer = None
ethersacn_api_keys = ["9ZAPKN2117CWZA9HYCGA2G47JBWMNXZY74","UDC782U5N41BXTTT4DQ9ZBCSQV68VVTWDY","KS5UJP2XGTADUTMNAKPDYZ4U5RWC1YFAQK"]


if __name__=="__main__":
    
    ethtx_config = EthTxConfig(
            mongo_connection_string="mongodb://localhost/ethtx",  ##MongoDB connection string,
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
    arr = create_arr(7098800,10)
    # pool = Pool(processes=20,initializer = init,initargs=('child',))
    # pool.map(log_produce, arr)
    for num in arr:
        start = time.time()
        log_produce(num)
        end = time.time()
        print("num = [%d] cost time %.2f"%(num,end-start))
    # print(create_arr(1,10))
