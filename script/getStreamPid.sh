#!/bin/bash
START_USR="mjw"
PROGRESS_KEY_WORD=$1
#PROGRESS_KEY_WORD="flink_streaming"
#PROGRESS_KEY_WORD="executionMode"
#echo $PROGRESS_KEY_WORD
#su - root -c "ps -fu $START_USR | grep \".*\"$PROGRESS_KEY_WORD\".*\" | sed -e \"s/^ *\"$START_USR\" *\([0-9]*\).*$/\1/\" | head -n 1"
#ps -fu $START_USR | grep ".*"$PROGRESS_KEY_WORD".*" | sed -e "s/^ *\"$START_USR\" *\([0-9]*\).*$/\1/" | head -n 1 
ps -fu $START_USR | grep ".*"$PROGRESS_KEY_WORD".*" | sed -e "s/^ *"$START_USR" *\([0-9]*\).*$/\1/" | head -n 1 > $2
#echo "ps -fu $START_USR | grep \".*\"${PROGRESS_KEY_WORD}\".*\" | sed -e \"s/^ *\"$START_USR\" *\([0-9]*\).*$/\1/\" | head -n 1"
