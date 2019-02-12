from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError
from hotels.errors import ClusterError, HdfsError
from hotels.booking_schema import booking_schema


def get_booking_data_frame(file_name, file_system="local", cluster_manager="local"):
    """
   Read .csv file with bookings from local or hdfs file system and convert to data frame

   :param str file_name: The person sending the message
   :param str file_system: Where will be searching the file, can be "local" or "hdfs"
   :param str cluster_manager: Which manager will be used, can be "local" or "yarn"
   :return: Booked and searched hotels
   :rtype: DataFrame
   :raises ValueError: if wrong params
   :raises ClusterError: some error with manager
   :raises FileNotFoundError: if file does not exist
   """

    # process file name for file system
    if file_system == "hdfs":
        file_name = "hdfs://{}".format(file_name)
    elif file_system == "local":
        pass
    else:
        raise ValueError("Wrong file_system, can be 'local' or 'hdfs'")

    # get spark session object
    try:
        spark = (
            SparkSession.builder.appName("booking")
            .master(cluster_manager)
            .getOrCreate()
        )
    except Exception as error:
        raise ClusterError(
            "Check cluster_manager argument or "
            "env variables HADOOP_CONF_DIR and YARN_CONF_DIR"
        ) from error

    # convert csv to data frame
    try:
        booking_data = spark.read.csv(
            file_name, header=True, schema=booking_schema, sep=","
        )
    except AnalysisException as error:
        raise FileNotFoundError("Path does not exist") from error
    except Py4JJavaError as error:
        raise HdfsError("Check HDFS file system") from error

    return booking_data
