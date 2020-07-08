from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime
from pyflink.table.udf import udf


@udf(input_types=[DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.FLOAT()], result_type=DataTypes.ROW())
def split(kpi_key, timestamp, value):
    return {'kpi_key': kpi_key, 'timestamp': timestamp, 'value': value, 'lower': value, 'upper': value, 'score': 0.0,
            'anomaly': 0}


get_str = udf(lambda x, y: x[y], [DataTypes.ROW(), DataTypes.STRING()], DataTypes.STRING())
get_bigint = udf(lambda x, y: x[y], [DataTypes.ROW(), DataTypes.STRING()], DataTypes.BIGINT())
get_float = udf(lambda x, y: x[y], [DataTypes.ROW(), DataTypes.STRING()], DataTypes.FLOAT())
get_int = udf(lambda x, y: x[y], [DataTypes.ROW(), DataTypes.STRING()], DataTypes.INT())


def from_kafka_to_kafka_demo():
    # init environment
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    s_env.set_parallelism(1)

    # use blink table planner
    st_env = StreamTableEnvironment \
        .create(s_env, environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .use_blink_planner().build())

    st_env.get_config().get_configuration().set_boolean('python.fn-execution.memory.managed', True)

    st_env.register_function('split', split)
    st_env.register_function('get_str', get_str)
    st_env.register_function('get_bigint', get_bigint)
    st_env.register_function('get_float', get_float)
    st_env.register_function('get_int', get_int)

    # register source and sink
    register_rides_source(st_env)
    register_rides_sink(st_env)

    # query
    st_env.from_path("source").select('split(kpi_key,timestamp,value) as res'). \
        select(
        'get_str(res,"kpi_key") as kpi_key,get_bigint(res,"timestamp") as timestamp,get_float(res,"value") as value,'
        'get_float(res,"lower") as lower,get_float(res,"upper") as upper, get_float(res,"score") as score,'
        'get_int(res,"anomaly") as anomaly').insert_into("sink")

    # execute
    st_env.execute("2-from_kafka_to_kafka")


def register_rides_source(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("0.11")
            .topic("Rides")
            .start_from_earliest()
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
            DataTypes.FIELD("kpi_key", DataTypes.STRING()),
            DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
            DataTypes.FIELD("value", DataTypes.FLOAT())]))) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("kpi_key", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("value", DataTypes.FLOAT())) \
        .in_append_mode() \
        .create_temporary_table("source")


def register_rides_sink(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("0.11")
            .topic("TempResults")
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
            DataTypes.FIELD("kpi_key", DataTypes.STRING()),
            DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
            DataTypes.FIELD("value", DataTypes.FLOAT()),
            DataTypes.FIELD("lower", DataTypes.FLOAT()),
            DataTypes.FIELD("upper", DataTypes.FLOAT()),
            DataTypes.FIELD("score", DataTypes.FLOAT()),
            DataTypes.FIELD("anomaly", DataTypes.INT())
        ]))) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("kpi_key", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("value", DataTypes.FLOAT())
            .field("lower", DataTypes.FLOAT())
            .field("upper", DataTypes.FLOAT())
            .field("score", DataTypes.FLOAT())
            .field("anomaly", DataTypes.INT())) \
        .in_append_mode() \
        .create_temporary_table("sink")


if __name__ == '__main__':
    from_kafka_to_kafka_demo()

"""
这一步确实实例化Kafka()的时候不会报错，但是后面编程时候会有很大问题 正确做法将 
flink-json-1.10.0-sql-jar.jar 
flink-sql-connector-kafka_2.11-1.10.0.jar 
kafka-clients-2.2.0.jar flink-jdbc_2.11-1.10.0.jar
这几个包复制到site-packages/pyflink/lib下就可以了 我的是/home/yy1s/project/test/lib/python3.6/site-packages/pyflink/lib 
/home/yy1s/project/test/lib/python3.6/site-packages/pyflink/lib [yy1s@hub lib]$ ls -lrt total 154404 -rw-r
w-r--. 1 yy1s yy1s 9931 Sep 2 2019 slf4j-log4j12-1.7.15.jar -rw-rw-r--. 1 yy1s yy1s 489884 Sep 2 2019 l
og4j-1.2.17.jar -rw-rw-r--. 1 yy1s yy1s 19301237 Feb 7 13:54 flink-table_2.11-1.10.0.jar -rw-rw-r--. 1 yy
s yy1s 22520058 Feb 7 13:54 flink-table-blink_2.11-1.10.0.jar -rw-rw-r--. 1 yy1s yy1s 110055308 Feb 7 13:54 flink-dist_2.11-1.10.0.jar -rw-r--r--. 1 yy1s yy1s 89695 Feb 7 14:51 flink-jdbc_2.11-1.10.0.jar3月前

"""
