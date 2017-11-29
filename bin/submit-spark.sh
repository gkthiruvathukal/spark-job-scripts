#! /bin/bash

# Usage:   submit-spark.sh <allocation> <time> <num_nodes> <queue>
# Example: submit-spark.sh SDAV 08:00:00 12 pubnet-nox11

if [ $# -lt 4 ]; then
    echo "Usage: submit-spark.sh <allocation> <time> <num_nodes> <queue>"
    echo "Example: submit-spark.sh SDAV 08:00:00 12 pubnet-nox11"
    exit -1
fi

allocation=$1
time=$2
nodes=$3
queue=$4


# submit

JOBID=$(qsub -n $nodes -t $time -A $allocation -q ${queue} /gpfs/mira-home/thiruvat/code/spark-job-scripts/bin/start-spark.sh)
SPARK_JOB_INFO=$HOME/spark-hostname-${COBALT_JOBID}.txt

if [ -e $SPARK_JOB_INFO ]; then
  rm -f $SPARK_JOB_INFO
fi

echo "Cobalt JOB $JOBID started"
echo "Spark Job Hostname (tempfile) is $SPARK_JOB_INFO"

count=0
while [ ! -e $SPARK_JOB_INFO ]; do 
  echo "Waiting for Spark to launch..."; sleep 3
  count=$((count+1))
  if [ $count -gt 200 ]
  then
    echo "Spark failed to launch within ten minutes."
    qdel $JOBID
    break
  fi
done

cat $SPARK_JOB_INFO
rm -f $SPARK_JOB_INFO
