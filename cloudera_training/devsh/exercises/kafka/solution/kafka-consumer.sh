#!/bin/bash

# Start the console consumer, receive all messages from the beginning of the topic
kafka-console-consumer   --zookeeper localhost:2181   --topic weblogs   --from-beginning

# Re-start the console consumer, receive only new messages
kafka-console-consumer   --zookeeper localhost:2181   --topic weblogs
  