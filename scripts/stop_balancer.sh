#!/bin/bash

PID=`sudo jps | grep "Balancer" | awk '{print $1}'`
kill -9 $PID
