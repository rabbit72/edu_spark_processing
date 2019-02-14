import sys

from hotels import read_data_frame_from_csv
from hotels.errors import ClusterError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (ByteType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)

booking_schema = StructType(
        [
            StructField("date_time", StringType()),
            StructField("site_name", IntegerType()),
            StructField("posa_continent", IntegerType()),
            StructField("user_location_country", IntegerType()),
            StructField("user_location_region", IntegerType()),
            StructField("user_location_city", IntegerType()),
            StructField("orig_destination_distance", DoubleType()),
            StructField("user_id", IntegerType()),
            StructField("is_mobile", ByteType()),
            StructField("is_package", IntegerType()),
            StructField("channel", IntegerType()),
            StructField("srch_ci", StringType()),
            StructField("srch_co", StringType()),
            StructField("srch_adults_cnt", StringType()),
            StructField("srch_children_cnt", IntegerType()),
            StructField("srch_rm_cnt", IntegerType()),
            StructField("srch_destination_id", IntegerType()),
            StructField("srch_destination_type_id", IntegerType()),
            StructField("is_booking", ByteType()),
            StructField("cnt", LongType()),
            StructField("hotel_continent", IntegerType()),
            StructField("hotel_country", IntegerType()),
            StructField("hotel_market", IntegerType()),
            StructField("hotel_cluster", IntegerType()),
        ]
    )


def get_booked_couples_hotels(data_frame, limit=3):
    """
   Find N most popular hotels between couples without children

   :param DataFrame data_frame: data with hotel booking and searching
   :param int limit: How many top hotels
   :return: Popular hotels between couples
   :rtype: DataFrame
   """
    cols = ["hotel_continent", "hotel_country", "hotel_market"]
    for_couples = (
        data_frame.select(cols)
        .filter(data_frame.srch_adults_cnt == 2)
        .groupBy(cols)
        .count()
        .orderBy(col("count").desc())
    )
    return for_couples.limit(limit)


def get_searched_booked_hotels_from_same_country(data_frame, limit=1):
    """
   Find N most popular countries where hotels are booked and searched from same country

   :param DataFrame data_frame: data with hotel booking and searching
   :param int limit: How many top hotels
   :return: Popular hotels
   :rtype: DataFrame
   """
    cols = ["hotel_country", "user_location_country"]
    data_frame_result = (
        data_frame.select(cols)
        .filter(data_frame.user_location_country == data_frame.hotel_country)
        .filter(data_frame.is_booking == 1)
        .groupBy(cols)
        .count()
        .orderBy(col("count").desc())
    )

    return data_frame_result.limit(limit)


def get_searched_hotels_with_children_not_booked(data_frame, limit=3):
    """
   Find top N hotels where people with children are interested but not booked in the end

   :param DataFrame data_frame: data with hotel booking and searching
   :param int limit: How many top hotels
   :return: Popular hotels
   :rtype: DataFrame
   """
    cols = ["hotel_continent", "hotel_country", "hotel_market"]
    data_frame_result = (
        data_frame.select(cols)
        .filter(data_frame.srch_children_cnt > 0)
        .filter(data_frame.is_booking == 0)
        .groupBy(cols)
        .count()
        .orderBy(col("count").desc())
    )

    return data_frame_result.limit(limit)


def main():
    try:
        csv_file = sys.argv[1]
    except IndexError:
        exit("Input file name as first argument")

    try:
        session = SparkSession.builder.appName("booking").getOrCreate()
    except Exception as error:
        raise ClusterError(
            "Check cluster_manager argument or "
            "env variables HADOOP_CONF_DIR and YARN_CONF_DIR"
        )

    booking_data_frame = read_data_frame_from_csv(
        csv_file,
        session,
        schema=booking_schema
    )

    methods = {
        "couples": get_booked_couples_hotels,
        "country": get_searched_booked_hotels_from_same_country,
        "children": get_searched_hotels_with_children_not_booked,
    }
    for method in methods.values():
        result = method(booking_data_frame)
        result.show()


if __name__ == "__main__":
    main()
