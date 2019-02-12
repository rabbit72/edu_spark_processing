from hotels import get_booking_data_frame
from pyspark.sql.functions import col


def get_popular_hotels_between_couples(data_frame, limit=3):
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


def main():
    booking_data_frame = get_booking_data_frame("./data/train.csv")
    between_couples = get_popular_hotels_between_couples(booking_data_frame)
    between_couples.show()


if __name__ == "__main__":
    main()
