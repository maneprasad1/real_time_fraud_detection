#!/bin/bash

#Name of tmux session
SESSION="kafka_producer"

#creating 1st window and running Zookeeper
tmux new-session -d -s $SESSION -n zookeeper
tmux send-keys -t $SESSION:0 'bin/zookeeper-server-start.sh config/zookeeper.properties' C-m

sleep 15

#creating 2nd window and running Kafka
tmux new-window -t $SESSION:1 -n kafka
tmux send-keys -t $SESSION:1 'bin/kafka-server-start.sh config/server.properties' C-m

sleep 15

#creating 3rd window and running Python script
tmux new-window -t $SESSION:2 -n producer
tmux send-keys -t $SESSION:2 'python3 stream_1k_s3.py' C-m

#creating 4th window
tmux new-window -t $SESSION:3 -n shell

#opening tmux
tmux attach -t $SESSION
