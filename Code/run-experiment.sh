./Start.sh
nohup python TransactionManager.py &
echo $! > save_pid.txt
sleep 2
python MainClass.py ../TestCase/Test1.txt
kill -9 `cat save_pid.txt`
./Stop.sh