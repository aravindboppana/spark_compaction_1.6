#!/usr/bin/env bash

HELP_STR="""

Please provide the arguments as follows

Example 1: Strategy: new
    sh spark_compaction.sh {compaction_strategy} {source_db_name} {source_table_name} {source_data_location} {target_db_name} {target_table_name} {target_data_location}

Example 2: Strategy: overwrite
    sh spark_compaction.sh {compaction_strategy} {source_db_name} {source_table_name} {source_data_location}

"""

compaction() {

    COMPACTION_STRATEGY="$1"
    SOURCE_LOCATION_HDFS="$2"
    TARGET_LOCATION_HDFS="$3"
    APP_NAME=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['app_name'];"`
    SPARK_MASTER=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['master'];"`
    SPARK_EXECUTOR_INSTANCES=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['spark_executor_instances'];"`
    SPARK_EXECUTOR_MEMORY=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['spark_executor_memory'];"`
    KEYTAB_LOCATION=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['kerberos']['keytab'];"`
    KERBEROS_PRINCIPAL=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['kerberos']['principal'];"`

    echo "APP_NAME: ${APP_NAME}"

    if [[ -z "${TARGET_DATA_LOCATION}" ]]; then
        if [[ "${SPARK_MASTER}" == "cluster" ]]; then
            echo "Launching Spark Streaming Application in Yarn Cluster Mode"
            SPARK_SUBMIT_STARTUP_CMD="spark-submit --keytab ${KEYTAB_LOCATION} --principal ${KERBEROS_PRINCIPAL} --master yarn-cluster --num-executors ${SPARK_EXECUTOR_INSTANCES} --executor-memory ${SPARK_EXECUTOR_MEMORY} --name ${APP_NAME} --files ${CONF_DIR}/application_configs.json --class com.clairvoyant.insight.bigdata.SparkCompaction_V1 ${JAR_FILE_LOCATION} ${COMPACTION_STRATEGY} ${SOURCE_LOCATION_HDFS}"
        else
            echo "Launching Spark Streaming Application in Yarn Client Mode"
            SPARK_SUBMIT_STARTUP_CMD="spark-submit --master yarn-client --num-executors ${SPARK_EXECUTOR_INSTANCES} --executor-memory ${SPARK_EXECUTOR_MEMORY} --driver-class-path ${CONF_DIR} --class com.clairvoyant.insight.bigdata.SparkCompaction_V1 ${JAR_FILE_LOCATION} ${COMPACTION_STRATEGY} ${SOURCE_LOCATION_HDFS}"
        fi
    else
        if [[ "${SPARK_MASTER}" == "cluster" ]]; then
            echo "Launching Spark Streaming Application in Yarn Cluster Mode"
            SPARK_SUBMIT_STARTUP_CMD="spark-submit --keytab ${KEYTAB_LOCATION} --principal ${KERBEROS_PRINCIPAL} --master yarn-cluster --num-executors ${SPARK_EXECUTOR_INSTANCES} --executor-memory ${SPARK_EXECUTOR_MEMORY} --name ${APP_NAME} --files ${CONF_DIR}/application_configs.json --class com.clairvoyant.insight.bigdata.SparkCompaction_V1 ${JAR_FILE_LOCATION} ${COMPACTION_STRATEGY} ${SOURCE_LOCATION_HDFS} ${TARGET_LOCATION_HDFS}"
        else
            echo "Launching Spark Streaming Application in Yarn Client Mode"
            SPARK_SUBMIT_STARTUP_CMD="spark-submit --master yarn-client --num-executors ${SPARK_EXECUTOR_INSTANCES} --executor-memory ${SPARK_EXECUTOR_MEMORY} --driver-class-path ${CONF_DIR} --class com.clairvoyant.insight.bigdata.SparkCompaction_V1 ${JAR_FILE_LOCATION} ${COMPACTION_STRATEGY} ${SOURCE_LOCATION_HDFS} ${TARGET_LOCATION_HDFS}"
        fi
    fi

    echo "executing: ${SPARK_SUBMIT_STARTUP_CMD}"
    eval ${SPARK_SUBMIT_STARTUP_CMD}

    if [[ $? -ne 0 ]]; then
        exit 0
    fi

}

invalidate_metadata_and_compute_stats() {

    DATABASE_NAME="$1"
    TABLE_NAME="$2"

    impala-shell -q "INVALIDATE METADATA ${DATABASE_NAME}.${TABLE_NAME}"
    impala-shell -q "COMPUTE STATS ${DATABASE_NAME}.${TABLE_NAME}"
    impala-shell -q "DESCRIBE EXTENDED ${DATABASE_NAME}.${TABLE_NAME}"
    impala-shell -q "SHOW TABLE STATS ${DATABASE_NAME}.${TABLE_NAME}"

}

COMPACTION_STRATEGY="$1"
SOURCE_DB_NAME="$2"
SOURCE_TABLE_NAME="$3"
SOURCE_DATA_LOCATION="$4"
TARGET_DB_NAME="$4"
TARGET_TABLE_NAME="$6"
TARGET_DATA_LOCATION="$7"

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LIB_DIR="${BIN_DIR}/../lib"
CONF_DIR="${BIN_DIR}/../conf"
JAR_FILE_LOCATION="${LIB_DIR}/spark-compaction-1.6-jar-with-dependencies.jar"
APPLICATION_CONF_FILE="${CONF_DIR}/application_configs.json"

SOURCE_DATA_BACKUP_LOCATION="${SOURCE_DATA_LOCATION}_backup"
COMPACTED_TEMP_LOCATION="${SOURCE_DATA_LOCATION}_temp"

if [[ -z "${COMPACTION_STRATEGY}" || -z "${SOURCE_DB_NAME}" || -z "${SOURCE_TABLE_NAME}" || -z ${SOURCE_DATA_LOCATION} ]]; then
    echo "Please provide all the arguments to proceed with the compaction Job. Please provide the arguments as follows"
    echo "${HELP_STR}"
    exit 0
fi

impala-shell -q "SELECT * FROM ${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME} LIMIT 1"
if [[ "$?=0" ]]; then
    echo "Source Database and table exists. Proceeding with the next task"
else
    echo "Source Database or Table doesn't exist. Exiting the process"
    exit 0
fi

echo "BIN_DIR: ${BIN_DIR}"
echo "LIB_DIR: ${LIB_DIR}"
echo "JAR_FILE_LOCATION: ${JAR_FILE_LOCATION}"
echo "APPLICATION_CONF_FILE: ${APPLICATION_CONF_FILE}"
echo "COMPACTION STRATEGY:  ${COMPACTION_STRATEGY}"
echo "SOURCE DB:  ${SOURCE_DB_NAME}"
echo "SOURCE TABLE:  ${SOURCE_TABLE_NAME}"

if [[ "${COMPACTION_STRATEGY}" == "new" ]]; then
    if [[ -z "${TARGET_DB_NAME}" || -z "${TARGET_TABLE_NAME}" ]]; then
        echo "##########################"
        echo "Please provide all the arguments to proceed with the compaction Job. Please provide the arguments as follows"
        echo "##########################"
        echo "${HELP_STR}"
        exit 0
    fi
    echo "TARGET DB: " ${TARGET_DB_NAME}
    echo "TARGET TABLE: " ${TARGET_TABLE_NAME}

    impala-shell -q "SELECT * FROM ${TARGET_DB_NAME}.${TARGET_TABLE_NAME} LIMIT 1"
    if [[ $? = 0 ]]; then
        echo "Target table already exists. Exiting the application"
        exit 0
    else
        echo "Target Table doesn't exist. Proceeding with the next task"
    fi

    echo "Starting Compaction"

    compaction "${COMPACTION_STRATEGY}" "${SOURCE_DATA_LOCATION}" "${TARGET_DATA_LOCATION}"

    if [[ $? -ne 0 ]]; then
        echo "Compaction Failed"
        exit 1
    fi

    echo "Create new external table as the one in the Source DB"
    impala-shell -q "CREATE EXTERNAL TABLE ${TARGET_DB_NAME}.${TARGET_TABLE_NAME} LIKE ${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME} STORED AS PARQUET LOCATION '${TARGET_DATA_LOCATION}' "
    echo "Created new External Table"

    impala-shell -q "SHOW TABLE STATS ${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME}"
    invalidate_metadata_and_compute_stats "${TARGET_DB_NAME}" "${TARGET_TABLE_NAME}"

    SOURCE_TABLE_COUNT="$(impala-shell -q 'select count(*) from '${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME}'' -B)"
    TARGET_TABLE_COUNT="$(impala-shell -q 'select count(*) from '${TARGET_DB_NAME}.${TARGET_TABLE_NAME}'' -B)"

    echo "SOURCE Table Count: ${SOURCE_TABLE_COUNT}"
    echo "TARGET Table Count: ${TARGET_TABLE_COUNT}"

    if [[ $? = 0 && ${SOURCE_TABLE_COUNT}=${TARGET_TABLE_COUNT}} ]]; then
        echo "SUCCESSFULLY COMPLETED COMPACTION"
    fi

elif [[ "${COMPACTION_STRATEGY}" = "overwrite" ]]; then

    impala-shell -q "SHOW TABLE STATS ${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME}"
    compaction "${COMPACTION_STRATEGY}" "${SOURCE_DATA_LOCATION}"

    SOURCE_TABLE_COUNT_BEFORE_COMPACTION="$(impala-shell -q 'select count(*) from '${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME}'' -B)"

    echo "Moving Source data to backup location"
    hadoop fs -mv ${SOURCE_DATA_LOCATION} ${SOURCE_DATA_BACKUP_LOCATION}
    if [[ $? = 0 ]]; then
        echo "Backed up Source data"
    fi

    echo "Copying Compacted files to Source Location"
    hadoop fs -mv ${COMPACTED_TEMP_LOCATION} ${SOURCE_DATA_LOCATION}
    echo "Moved Compacted Data to Source Location"

    invalidate_metadata_and_compute_stats "${SOURCE_DB_NAME}" "${SOURCE_TABLE_NAME}"
    SOURCE_TABLE_COUNT_AFTER_COMPACTION="$(impala-shell -q 'select count(*) from '${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME}'' -B)"

    echo "Table Count before Compaction: ${SOURCE_TABLE_COUNT_BEFORE_COMPACTION}"
    echo "Table Count after Compaction: ${SOURCE_TABLE_COUNT_AFTER_COMPACTION}"

    if [[ $? = 0 && ${SOURCE_TABLE_COUNT_BEFORE_COMPACTION}=${SOURCE_TABLE_COUNT_AFTER_COMPACTION} ]]; then

        echo "Deleting Compacted files in Temp location"
        hadoop fs -rm -R ${COMPACTED_TEMP_LOCATION}
        echo "Deleting Source Data Backup"
        hadoop fs -rm -R ${SOURCE_DATA_BACKUP_LOCATION}
    else
        echo "Moving Source back to Source location as the row counts are not equal"
        hadoop fs -mv ${SOURCE_DATA_BACKUP_LOCATION} ${SOURCE_DATA_LOCATION}
        invalidate_metadata_and_compute_stats "${SOURCE_DB_NAME}" "${SOURCE_TABLE_NAME}"

    fi

    if [[ $? = 0 ]]; then
        echo "SUCCESSFULLY COMPLETED COMPACTION"
    fi

else
    echo "##########################"
    echo "Provide new or overwrite as compaction strategies"
    echo "##########################"
    exit 0
fi

