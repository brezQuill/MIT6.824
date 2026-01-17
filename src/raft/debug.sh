#!/bin/bash

begin=开始
end=结束
names=("选举" "AppendEntries" "sendAppend" "sendAppendEntries" "RequestVote" "拉票")

printf "%s = %d %s = %d\n" "$begin" "$(grep -c "$begin" log2D.txt)" "$end" "$(grep -c "$end" log2D.txt)"

for name in "${names[@]}"; do
    printf "%s = %d %s = %d\n" "$name$begin" "$(grep -c "$name$begin" log2D.txt)" "$end" "$(grep -c "$name$end" log2D.txt)"
done

printf "%s = %d\n" "副作用心跳" "$(grep -c "副作用心跳" log2D.txt)"
printf "%s = %d\n" "正常心跳" "$(grep -c "正常心跳" log2D.txt)"



cat log2D.txt | grep 每个server > serverCommit.txt 
cat log2D.txt | grep -v -e "##" -e 每个server > common.txt
cat log2D.txt | grep "4##" | grep -v -e 开始 -e 结束 > action_4.txt
cat log2D.txt | grep "3##" | grep -v -e 开始 -e 结束 > action_3.txt
cat log2D.txt | grep "old" > old.txt
cat log2D.txt | grep 当选 > 当选.txt
cat log2D.txt | grep 选举开始 > 选举开始.txt

cat log2D.txt | grep 花费时间 > 花费时间.txt                              
cat 花费时间.txt | grep persist > 花费时间_persist.txt
cat 花费时间.txt | grep RequestVote > 花费时间_RequestVote.txt
cat 花费时间.txt | grep AppendEntries > 花费时间_AppendEntries.txt
cat 花费时间.txt | grep old > 花费时间_old.txt
cat 花费时间.txt | grep 抢锁花费 > 抢锁花费时间.txt
