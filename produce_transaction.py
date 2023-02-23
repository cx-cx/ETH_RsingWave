# 较为原始的生产代码
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

ethtx_config = EthTxConfig(
    mongo_connection_string="mongomock://localhost/ethtx",  ##MongoDB connection string,
    etherscan_api_key="UDC782U5N41BXTTT4DQ9ZBCSQV68VVTWDY",  ##Etherscan API key,
    web3nodes={
        "mainnet": {
            "hook": "https://sly-chaotic-shape.discover.quiknode.pro/",  # multiple nodes supported, separate them with comma || http://localhost:8545
            "poa": False  # represented by bool value
        }
    },
    default_chain="mainnet",
    etherscan_urls={"mainnet": "https://api.etherscan.io/api", },
)

ethtx = EthTx.initialize(ethtx_config)
web3provider = ethtx.providers.web3provider
w3 = Web3(Web3.HTTPProvider('https://sly-chaotic-shape.discover.quiknode.pro/'))

def analysis_params(param_list):
    analysis = []
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


ethtx_config = EthTxConfig(
    mongo_connection_string="mongomock://localhost/ethtx",  ##MongoDB connection string,
    etherscan_api_key="UDC782U5N41BXTTT4DQ9ZBCSQV68VVTWDY",  ##Etherscan API key,
    web3nodes={
        "mainnet": {
            "hook": "https://sly-chaotic-shape.discover.quiknode.pro/",  # multiple nodes supported, separate them with comma || http://localhost:8545
            "poa": False  # represented by bool value
        }
    },
    default_chain="mainnet",
    etherscan_urls={"mainnet": "https://api.etherscan.io/api", },
)

ethtx = EthTx.initialize(ethtx_config)
web3provider = ethtx.providers.web3provider
w3 = Web3(Web3.HTTPProvider('https://sly-chaotic-shape.discover.quiknode.pro/'))

w3block: W3Block = web3provider.get_block(8325805)

block: Block = Block.from_raw(
    w3block=w3block,
    chain_id="mainnet",
)

results = {}
first = 0
producer = KafkaProducer(bootstrap_servers="localhost:9092",
         value_serializer=lambda m: json.dumps(m).encode())


for tx in w3block.transactions:
    tx = tx.hex()
    first = tx
    result = {}
    
    w3txdetail = w3.eth.get_transaction(tx)
    w3txreceipt = w3.eth.get_transaction_receipt(tx)
    w3transaction: W3Transaction = web3provider.get_transaction(tx)
    w3receipt: W3Receipt = web3provider.get_receipt(tx)
    w3calls: W3CallTree = web3provider.get_calls(tx)

    # read the raw transaction from the node
    transaction = Transaction.from_raw(w3transaction=w3transaction, w3receipt=w3receipt, w3calltree=w3calls)
    # get proxies used in the transaction
    proxies = ethtx.decoders.get_proxies(transaction.root_call, "mainnet")
    
    # decode transaction components
    abi_decoded_events: List[Event] = ethtx.decoders.abi_decoder.decode_events(
        transaction.events, block.metadata, transaction.metadata
    )
    abi_decoded_calls: DecodedCall = ethtx.decoders.abi_decoder.decode_calls(
        transaction.root_call, block.metadata, transaction.metadata, proxies
    )
    abi_decoded_transfers: List[
        DecodedTransfer
    ] = ethtx.decoders.abi_decoder.decode_transfers(abi_decoded_calls, abi_decoded_events)
    abi_decoded_balances: List[DecodedBalance] = ethtx.decoders.abi_decoder.decode_balances(
        abi_decoded_transfers
    )

    # semantically decode transaction components
    decoded_metadata: DecodedTransactionMetadata = (
        ethtx.decoders.semantic_decoder.decode_metadata(
            block.metadata, transaction.metadata, "mainnet"
        )
    )
    decoded_events: List[DecodedEvent] = ethtx.decoders.semantic_decoder.decode_events(
        abi_decoded_events, decoded_metadata, proxies
    )
    decoded_calls: Call = ethtx.decoders.semantic_decoder.decode_calls(
        abi_decoded_calls, decoded_metadata, proxies
    )
    decoded_transfers: List[
        DecodedTransfer
    ] = ethtx.decoders.semantic_decoder.decode_transfers(
        abi_decoded_transfers, decoded_metadata
    )
    decoded_balances: List[
        DecodedBalance
    ] = ethtx.decoders.semantic_decoder.decode_balances(
        abi_decoded_balances, decoded_metadata
    )

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

    

    # result['log'] = {'event': analysis_events(decoded_events),
    #                 'call': analysis_calls(decoded_calls), 
    #                 'transfer': analysis_transfers(decoded_transfers), 
    #                 'balance': analysis_balances(decoded_balances)}
    # producer.send('cx_test_transaction', result)
    print(result)
    # producer.send('cx_test_transaction', result)
    for event in analysis_events(decoded_events,tx):
        producer.send('cx_test_log', event)
        print("****-------****")
        print(event)
        print("****-------****")

    results[tx] = result
    print("--------------\n\n")


# 对数据进行处理
# tx_dict = dict(tx)
# tx_json = json.dumps(tx_dict, cls=HexJsonEncoder) # 去除其中的Hexsh字段和序列化为格式正确的json
# tx_json_load = json.loads(tx_json)  # 反序列化为python对象
# tx_dict = dict(tx_json_load)    # 转化为字典对象
# tx_dict["totalDifficulty"] = str(tx_dict["totalDifficulty"])    # 将totalDifficulty字段更改为string类型避免数字过大无法识别

# producer = KafkaProducer(bootstrap_servers="localhost:9092",
#          value_serializer=lambda m: json.dumps(m).encode())

# producer.send('cx_test_varchar', tx_dict)



