run.sh

#!/bin/sh
ps aux |grep python3 | grep productor |awk '{print $2 }' |xargs kill -9
ps aux |grep python3 | grep consumer |awk '{print $2 }' |xargs kill -9
python3 -u eth_productor.py cx_test_topic_20221201_1 >> productor_cx_test_topic_20221201_1.log 2>&1 &
sleep 1
python3 -u eth_consumer.py cx_test_topic_20221201_1 >> consumer_cx_test_topic_20221201_1.log 2>&1 &