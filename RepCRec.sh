#!/bin/bash
./Start.sh
nohup python TransactionManager.py &
echo $! > save_pid.txt
python MainClass.py /home/soumya/RepCRec/TestCase/Test8.txt
kill -9 `cat save_pid.txt`
