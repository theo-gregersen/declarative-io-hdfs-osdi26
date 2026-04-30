#!/bin/bash

# <start time> <src directory> <dest directory> <max batch bytes> <max bytes> <sleep between batches>

sleep $1

RAND_ADD=`tr -dc A-Za-z0-9 </dev/urandom | head -c 13`
MAX_BATCH_BYTES=$4
MAX_BYTES=$5
batch=()
batch_bytes=0
total_bytes=0

${HADOOP_HOME}/bin/hdfs dfs -ls -R $2 | awk '$1 ~ /^-/ {print $5, $8}' > tmpls-${RAND_ADD}.txt

echo "Starting transcoding from $2 to $3"

while IFS="" read -r p || [ -n "$p" ]
do
	size=$(echo "$p" | awk '{print $1}')
	filename=$(echo "$p" | awk '{print $2}')
	
	if [ $(($total_bytes + $size)) -gt $MAX_BYTES ]; then
		continue
	fi

	if [ $(($batch_bytes + $size)) -gt $MAX_BATCH_BYTES ] && [ ${#batch[@]} -gt 0 ]; then
		for f in "${batch[@]}"; do
			${PROJ_SCRIPTS}/simple_transcode.sh $f $3
		done
		batch=()
		batch_bytes=0
		sleep $6
	fi

	batch+=("$filename")
	batch_bytes=$(($batch_bytes + $size))
	total_bytes=$(($total_bytes + $size))

done < tmpls-${RAND_ADD}.txt

# handle last batch
if [ ${#batch[@]} -gt 0 ]; then
	for f in "${batch[@]}"; do
		${PROJ_SCRIPTS}/simple_transcode.sh $f $3
	done
fi

echo "Done transcoding"

