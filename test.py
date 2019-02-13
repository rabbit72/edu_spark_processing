import pytest
import main
from hotels.core import read_data_frame_from_csv, get_spark_session
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException


@pytest.fixture()
def data_frame():
    small_data_frame = "./tests/test_train.csv"
    test_session = get_spark_session()
    return read_data_frame_from_csv(small_data_frame, test_session)


@pytest.fixture()
def spark_session():
    return get_spark_session()


def test_booked_couples_hotels(data_frame):
    right_answer = [Row(hotel_continent=2, hotel_country=50, hotel_market=1457, count=6)]

    result = main.get_booked_couples_hotels(data_frame, limit=1)
    rows = result.collect()

    for row, right_row in zip(rows, right_answer):
        assert row.asDict() == right_row.asDict()


def test_searched_booked_hotels_from_same_country(data_frame):
    right_answer = [Row(hotel_country=66, user_location_country=66, count=3)]

    result = main.get_searched_booked_hotels_from_same_country(data_frame)
    rows = result.collect()

    for row, right_row in zip(rows, right_answer):
        assert row.asDict() == right_row.asDict()


def test_searched_hotels_with_children_not_booked(data_frame):
    right_answer = [
        Row(hotel_continent=2, hotel_country=50, hotel_market=1457, count=3),
        Row(hotel_continent=2, hotel_country=100, hotel_market=1457, count=2),
        Row(hotel_continent=1, hotel_country=50, hotel_market=1457, count=1),
    ]
    result = main.get_searched_hotels_with_children_not_booked(data_frame)
    rows = result.collect()

    for row, right_row in zip(rows, right_answer):
        assert row.asDict() == right_row.asDict()


def test_get_booking_data_frame_value_error(spark_session):
    with pytest.raises(ValueError):
        read_data_frame_from_csv(
            "./tests/test_train.csv", spark_session, file_system="error"
        )


def test_get_booking_data_frame_not_found(spark_session):
    with pytest.raises(AnalysisException):
        read_data_frame_from_csv("./tests/test_train.cs", spark_session)
