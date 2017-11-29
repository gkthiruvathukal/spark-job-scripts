#! /bin/bash

# This script is generated. Do not edit.

if [ -z "${SPARK_HOME}" ]; then
   export SPARK_HOME=/home/thiruvat/Work/spark
fi

if [ -z "${SPARK_CONF_DIR}" ]; then
   export SPARK_CONF_DIR=/home/thiruvat/Work/spark/conf
fi

if [ -z "${SPARK_SLAVES}" ]; then
   export SPARK_SLAVES=/home/thiruvat/Work/spark/conf/slaves.${COBALT_JOBID}
fi

if [ -z "${SPARK_LOG_DIR}" ]; then
   export SPARK_SLAVES=/home/thiruvat/Work/spark/logs
fi

cp $COBALT_NODEFILE $SPARK_SLAVES
cd $SPARK_HOME

echo "Starting Apache Spark as Normal"
./sbin/start-all.sh 

echo "Apache Spark Started"

master=$(hostname)
num_workers=$(wc -l ${SPARK_SLAVES})

SPARK_JOB_INFO=$HOME/spark-hostname-$COBALT_JOBID
SPARK_JOB_INFO="$HOME/spark-hostname-${COBALT_JOBID}.txt"

echo "# Spark is now running with $num_workers workers:" > $SPARK_JOB_INFO
echo "debug: COBALT_NODEFILE=${COBALT_NODEFILE}" >> $SPARK_JOB_INFO
echo "debug: SPARK_HOME=${SPARK_HOME}" >> $SPARK_JOB_INFO
echo "debug: SPARK_CONF_DIR=${SPARK_CONF_DIR}" >> $SPARK_JOB_INFO
echo "debug: SPARK_LOG_DIR=${SPARK_LOG_DIR}" >> $SPARK_JOB_INFO
echo "debug: SPARK_SLAVES=${SPARK_SLAVES}" >> $SPARK_JOB_INFO
echo ""
echo "export SPARK_STATUS_URL=http://${master}.cooley.pub.alcf.anl.gov:8000" >> $SPARK_JOB_INFO
echo "export SPARK_MASTER_URI=spark://${master}:7077" >>  $SPARK_JOB_INFO

# Needed to keep Cobalt alive

while true
do
  sleep 30
done
