#!/bin/bash
# random_test is a tool to check the sanity of media-server, including origin
# server and proxy server. And it also checks the status of server, such as
# cpu and memory usage.
# Author: Jiashun Zhu(zhujiashun2010@gmail.com)

if [ $# -eq 0 ]
then
    echo "Usgae: bash script.sh <test_duration_time> [<cpu_threshold> <mem_threshold>]"
    exit 1
fi

ms=./media_server
rt=./random_test
host=127.0.0.1
inner_port=8900
outer_port=8901
publishcdn_port=8906
playcdn1_port=8907
playcdn2_port=8904
playcdn3_port=8905
pid_list=()
port_list=()
export TCMALLOC_SAMPLE_PARAMETER=524288 # required by heap profiler

nohup $ms -port=$inner_port &
pid_list+=(`echo $!`)
port_list+=($inner_port)

nohup $ms -port=$publishcdn_port -proxy_to=$host:$inner_port &
pid_list+=(`echo $!`)
port_list+=($publishcdn_port)

nohup $ms -port=$outer_port -proxy_to=$host:$inner_port &
pid_list+=(`echo $!`)
port_list+=($outer_port)

nohup $ms -port=$playcdn1_port -proxy_to=$host:$outer_port &
pid_list+=(`echo $!`)
port_list+=($playcdn1_port)

nohup $ms -port=$playcdn2_port -proxy_to=$host:$outer_port &
pid_list+=(`echo $!`)
port_list+=($playcdn2_port)

nohup $ms -proxy_to=list://$host:$playcdn1_port,$host:$playcdn2_port -proxy_lb=rr -port=$playcdn3_port &
pid_list+=(`echo $!`)
port_list+=($playcdn3_port)

kill_all() {
    for ((i=0; i<${#pid_list[@]}; i++));
    do
        kill -9 ${pid_list[$i]}
    done
}

test_time=$1
echo "Testting..."
# pressure testing
nohup $rt -play_server=$host:$inner_port -push_server=$host:$inner_port --test_duration_s=$test_time -dummy_port=8910 &
nohup $rt -play_server=$host:$outer_port -push_server=$host:$inner_port --test_duration_s=$test_time -dummy_port=8911 &
nohup $rt -play_server=$host:$playcdn1_port -push_server=$host:$publishcdn_port --test_duration_s=$test_time -dummy_port=8912 &
nohup $rt -play_server=$host:$playcdn3_port -push_server=$host:$inner_port --test_duration_s=$test_time -dummy_port=8913 &

# functional testing
./rtmp_press --push_server=127.0.0.1:8906 --play_server=127.0.0.1:8905 --push_vhost=test.com --play_vhost=test.com --match_duration=$test_time
if [ $? -ne 0 ]; then
    echo "Fail to pass functional testing"
    kill_all
    exit 1
fi

echo "[OK] Passing functional testing..."

# cpu and mem testing
for ((i=0; i<${#pid_list[@]}; i++));
do
    t=`ps -p ${pid_list[$i]} -o %cpu,rss | tail -1`
    IFS=' ' read -r -a array <<< $t

    cpu_threshold=35  # it means 35% cpu usage
    if [ -n "$2" ]; then
        cpu_threshold=$2
    fi
    cpu_use=`echo ${array[0]}'>'$cpu_threshold | bc -l`
    if [ $cpu_use -eq 1 ]; then
        echo "CPU usage of process is greater than $cpu_threshold%, cpu profile:"
        curl 127.0.0.1:${port_list[$i]}/hotspots/cpu
        kill_all
        exit 1
    fi

    mem_threshold=400000 #400M
    if [ -n "$3" ]; then
        mem_threshold=$3
    fi
    mem_use=`echo ${array[1]}'>'$mem_threshold | bc -l`
    if [ $mem_use -eq 1 ]; then
        echo "Mem usage of process is greater than $mem_threshold, mem profile:"
        curl 127.0.0.1:${port_list[$i]}/hotspots/heap
        kill_all
        exit 1
    fi
done

echo "[OK] Passing cpu and mem testing..."

# stability testing
core_rc=0
modules=(inner publish_cdn outer play_cdn1 play_cdn2 play_cdn3)
for ((i=0; i<${#pid_list[@]}; i++));
do
    if ps -p ${pid_list[$i]} > /dev/null
    then
       echo "${modules[$i]} is running"
    else
       echo "${modules[$i]} is not running"
       core_rc=1
    fi
done

if [ $core_rc -ne 0 ]; then
    echo "Fail to pass stability testing, one process has failed"
    kill_all
    exit $core_rc
fi

echo "[OK] Passing stability testing..."
echo "Passing all tests!"
kill_all

exit 0
