import pytest
import main
from hotels.core import get_booking_data_frame, get_spark_session
from pyspark.sql import Row


@pytest.fixture()
def data_frame():
    small_data_frame = "./tests/test_train.csv"
    test_session = get_spark_session()
    return get_booking_data_frame(small_data_frame, test_session)


@pytest.fixture()
def spark_session():
    return get_spark_session()


def test_booked_couples_hotels(data_frame):
    right_answer = [Row(hotel_continent=2, hotel_country=66, hotel_market=628, count=3)]

    result = main.get_booked_couples_hotels(data_frame, limit=1)
    rows = result.collect()

    for row, right_row in zip(rows, right_answer):
        assert row["count"] == right_row["count"]
        assert row["hotel_continent"] == right_row["hotel_continent"]
        assert row["hotel_country"] == right_row["hotel_country"]
        assert row["hotel_market"] == right_row["hotel_market"]


def test_searched_booked_hotels_from_same_country(data_frame):
    right_answer = [Row(hotel_country=50, user_location_country=50, count=6)]

    result = main.get_searched_booked_hotels_from_same_country(data_frame)
    rows = result.collect()

    for row, right_row in zip(rows, right_answer):
        assert row["count"] == right_row["count"]
        assert row["hotel_country"] == right_row["hotel_country"]
        assert row["user_location_country"] == right_row["user_location_country"]


def test_searched_hotels_with_children_not_booked(data_frame):
    right_answer = [
        Row(hotel_continent=2, hotel_country=50, hotel_market=1457, count=3),
        Row(hotel_continent=2, hotel_country=100, hotel_market=1457, count=2),
        Row(hotel_continent=1, hotel_country=50, hotel_market=1457, count=1),
    ]
    result = main.get_searched_hotels_with_children_not_booked(data_frame)
    rows = result.collect()

    for row, right_row in zip(rows, right_answer):
        assert row["count"] == right_row["count"]
        assert row["hotel_continent"] == right_row["hotel_continent"]
        assert row["hotel_country"] == right_row["hotel_country"]
        assert row["hotel_market"] == right_row["hotel_market"]


def test_get_booking_data_frame_value_error(spark_session):
    with pytest.raises(ValueError):
        get_booking_data_frame(
            "./tests/test_train.csv", spark_session, file_system="error"
        )


def test_get_booking_data_frame_not_found(spark_session):
    with pytest.raises(FileNotFoundError):
        get_booking_data_frame("./tests/test_train.cs", spark_session)
