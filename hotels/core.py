from hotels.errors import HdfsError
from py4j.protocol import Py4JJavaError


def read_data_frame_from_csv(
    file_name, spark_session, file_system="local", schema=None
):
    """
   Read .csv file with bookings from local or hdfs file system and convert to data frame

   :param str file_name: The person sending the message
   :param str file_system: Where will be searching the file, can be "local" or "hdfs"
   :param SparkSession spark_session: Spark session
   :param StructType schema: schema for csv
   :return: Booked and searched hotels
   :rtype: DataFrame
   :raises ValueError: if wrong params
   :raises FileNotFoundError: if file does not exist
   """

    # process file name for file system
    if file_system == "hdfs":
        file_name = "hdfs://{}".format(file_name)
    elif file_system == "local":
        pass
    else:
        raise ValueError("Wrong file_system, can be 'local' or 'hdfs'")

    # convert csv to data frame
    try:
        booking_data = spark_session.read.csv(
            file_name, header=True, schema=schema, sep=","
        )
    except Py4JJavaError as error:
        raise HdfsError("Check HDFS file system")

    return booking_data
