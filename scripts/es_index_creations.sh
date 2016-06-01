#!/bin/bash

export ES_NODE_ADDRESS="192.168.56.1"
export ES_NODE_PORT="9200"

export INDEX_NAME="hw2"
export TYPE_NAME="clicks"

export INDEX_JSON_FILE=index_desc.json

echo "Create elasticsearch index 'hw2' with type 'clicks'"

curl -XPUT $ES_NODE_ADDRESS:$ES_NODE_PORT/$INDEX_NAME -d @$INDEX_JSON_FILE

#echo "Update mappings for index $INDEX_NAME"
#curl -XPUT $ES_NODE_ADDRESS:$ES_NODE_PORT/$INDEX_NAME -d @$INDEX_JSON_FILE