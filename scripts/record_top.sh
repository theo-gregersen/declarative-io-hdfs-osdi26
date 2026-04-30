#!/bin/bash

while true; do
	date
	top -b -n1 | head -17
	sleep 10
done

