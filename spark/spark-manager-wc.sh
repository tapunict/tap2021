#!/bin/bash
[[ -z "${SPARK_ACTION}" ]] && { echo "SPARK_ACTION required"; exit 1; }

# ACTIONS start-zk, start-kafka, create-topic, 

echo "Running action ${SPARK_ACTION}"
case ${SPARK_ACTION} in
"wordcount")
python3 /opt/tap/wordcount.py -f $TAP_CODE -c $CORES -s $SEPARATOR
;;
"bash")
while true
do
	echo "Keep Alive"
	sleep 10
done
;;
esac

