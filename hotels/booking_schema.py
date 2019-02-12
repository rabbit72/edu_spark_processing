from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, ByteType, LongType

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