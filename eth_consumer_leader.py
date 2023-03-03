from kafka import KafkaConsumer
from kafka import KafkaProducer
from queue import Queue
import copy
import sys
import json

# TODO
# 1.get data\message from consumer -> aggregation_consumer
# 2.load_info -> map
# 3.check batch compile -> map
# 4.dispatch message to pipeline_helper -> sync_producer
# 5.dispatch batch data to RsingWave -> rinsgWave_producer

            
if __name__=="__main__":
    uid = sys.argv[1]

    aggregation_topic = f'transaction_test_{uid}_leader'.format(uid = uid)
    rinsgwave_producer_topic = f'transaction_test_{uid}'.format(uid = uid)
    rsingwave_sync_topic = f'pipeline_sync_{uid}'.format(uid=uid)

    aggregation_consumer = KafkaConsumer(aggregation_topic,
                            bootstrap_servers="localhost:9092",
                            group_id="test3",
                            auto_offset_reset='earliest')

    rinsgWave_producer = KafkaProducer(bootstrap_servers="localhost:9092",
                            value_serializer=lambda m: json.dumps(m).encode())

    sync_producer = KafkaProducer(bootstrap_servers="localhost:9092",
                            value_serializer=lambda m: json.dumps(m).encode())

    sync_job_queue = Queue()
    cur_process_batch = None
    global_recv_map = {}
    check_flag = 0
    complete_counter = 0
    delay = 0
    tolerante_delay = 10

    
    for msg in aggregation_consumer:
        info = json.loads(msg.value)
        # 1.add task into sync_job_queue
        if info.get("type",None) == "sync":
            print(info)
            sync_elem = copy.deepcopy(info)
            sync_elem.pop("type")
            sync_job_queue.put(sync_elem)
            continue
        # get task from sync_job_queue
        if cur_process_batch == None and not sync_job_queue.empty():
            cur_process_batch = sync_job_queue.get()

        # add info into global_recv_map
        global_recv_map[info["block_number"]] = info
        print("get info from block_number = ",info["block_number"])

        # if number == batch_last_number check batch complete
        if cur_process_batch != None and cur_process_batch["end"] == info["block_number"]:
            check_flag = 1
        if check_flag == 1:

            for i in range (cur_process_batch["start"],cur_process_batch["end"]+1):
                if global_recv_map.get(i,None) != None:
                    complete_counter += 1
                else :
                    complete_counter = 0
                    delay += 1
                    break
            
            if complete_counter == cur_process_batch["end"] - cur_process_batch["start"] + 1:

                for i in range (cur_process_batch["start"],cur_process_batch["end"]+1):
                    for result in global_recv_map[i]["transactions"].values():
                        rinsgWave_producer.send(rinsgwave_producer_topic, result)
                        # print(result)
                
                for i in range (cur_process_batch["start"],cur_process_batch["end"]+1):
                    global_recv_map.pop(i)
                
                sync_producer.send(rsingwave_sync_topic,cur_process_batch)
                print("send sync message topic = ",rsingwave_sync_topic)
                # reset task -> 2
                cur_process_batch = None
                complete_counter = 0
                check_flag = 0
                delay = 0

            if delay >= tolerante_delay:
                print("error batch not complete from [{0}] -> [{1}]".format(cur_process_batch["start"],cur_process_batch["end"]))

    