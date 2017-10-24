#!/bin/bash

# Start console producer
kafka-console-producer   --broker-list localhost:9092   --topic weblogs

# Sample test data, paste into terminal window:
# 3.94.78.5 - 69827 [15/Sep/2013:23:58:36 +0100] "GET /KBDOC-00033.html HTTP/1.0" 200 14417 "http://www.loudacre.com"  "Loudacre Mobile Browser iFruit 1"
