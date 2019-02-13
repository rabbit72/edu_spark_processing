from hotels import read_data_frame_from_csv, get_spark_session
from pyspark.sql.functions import col
import sys


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

    session = get_spark_session()
    booking_data_frame = read_data_frame_from_csv(csv_file, session)

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
