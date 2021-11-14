#!/bin/bash
lineNum=$(wc -l speed)
echo $lineNum
declare -i count
time=0
c=$(cat errorLog | wc -l)
echo errorNum=$c
cat speed | awk '{print int($11)}' >> M
c=$(cat M | wc -l)
echo totalMinutes=$c min
count=0
minValue=1000000
ub=58000
while read line
do
        #echo $line | grep "Mer" >> minT
        #if [ "$minT" != "" ] ; then
        #       echo $mint | awk '{print int($10)}' >> minT
        #fi
        #if [ -n "$minT" ] && [[ $minT -lt 81 ]] ; then
        #if [ "$minT" != "" ] && [[ $minT -lt 81 ]] ; then
        #       ((count++))
        #       echo count=$count
        #       echo minT= $minT
                #echo time=$time
        #       time= $(( time + minT ))
        #elif [ "$minT" = "" ]
        #then
        #       echo $minT is null
        #fi
        #echo $line
        if [[ $line -lt $ub ]] ; then
                ((count++))
                #echo time=$time
                time=`expr $time + $line`
                if [[ $line -lt $minValue ]] ; then
                        minValue=$line
                fi

        fi
done < M
rm M

echo 有效的count=$count
#echo time=$time
echo upperbound=$ub
echo bestSpeed=$minValue
echo -e "AverageTime=\c"  
echo "scale=2; $time/$count" |bc
echo

cat speed | grep "numSubInserted" | awk '{print int($16)}' >> T
c=$(cat T | wc -l)
echo totalMinutes_threads=$c min
count=0
minValue=1000000
# ub=87000
while read line
do
    if [[ $line -lt $ub ]] ; then
            ((count++))
            time=`expr $time + $line`
            if [[ $line -lt $minValue ]] ; then
                    minValue=$line
            fi

    fi
done < T
rm T
echo 有效的count_threads=$count
echo upperbound_threads=$ub
echo bestSpeed_threads=$minValue
echo -e "AverageTime_threads=\c"  
echo "scale=2; $time/$count" |bc
