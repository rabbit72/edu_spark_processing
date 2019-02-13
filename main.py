from hotels import get_booking_data_frame, get_spark_session
from pyspark.sql.functions import col
import click


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
        .filter(data_frame.is_booking == 1)
        .filter(data_frame.srch_children_cnt == 0)
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


@click.command()
@click.argument("csv_file")
@click.option(
    "--manager", "-m", type=click.Choice(["local[*]", "yarn"]), default="local[*]"
)
@click.option(
    "--file_system", "-fs", type=click.Choice(["local", "hdfs"]), default="local"
)
@click.option(
    "--query",
    "-q",
    type=click.Choice(["all", "couples", "country", "children"]),
    default="all",
)
def main(csv_file, manager, file_system, query):
    session = get_spark_session(cluster_manager=manager)
    booking_data_frame = get_booking_data_frame(
        csv_file, session, file_system=file_system
    )

    methods = {
        "couples": get_booked_couples_hotels,
        "country": get_searched_booked_hotels_from_same_country,
        "children": get_searched_hotels_with_children_not_booked
    }
    if query == "all":
        for method in methods.values():
            result = method(booking_data_frame)
            result.show()
    else:
        methods[query](booking_data_frame).show()


if __name__ == "__main__":
    main()
