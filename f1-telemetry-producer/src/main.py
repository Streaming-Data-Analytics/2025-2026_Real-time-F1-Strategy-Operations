import json
import logging
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from fastf1 import Cache, get_session


DRIVERS = ["VER", "LEC", "SAI"]
TOPIC_NAME = "f1-telemetry"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def configure_cache() -> Path:
    cache_dir = Path(__file__).resolve().parents[2] / "data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    Cache.enable_cache(str(cache_dir))
    return cache_dir


def load_session():
    session = get_session(2023, "Italian Grand Prix", "R")
    session.load()
    return session


def build_driver_dataframe(session, driver: str) -> pd.DataFrame:
    driver_laps = session.laps.pick_drivers(driver)

    telemetry = driver_laps.get_car_data().copy()
    position = driver_laps.get_pos_data().copy()

    telemetry_columns = ["Date", "SessionTime", "Speed", "RPM", "Throttle", "Brake", "nGear"]
    position_columns = ["Date", "SessionTime", "X", "Y", "Z"]

    telemetry = telemetry[[column for column in telemetry_columns if column in telemetry.columns]]
    position = position[[column for column in position_columns if column in position.columns]]

    if "Date" not in telemetry.columns or "Date" not in position.columns:
        raise ValueError(f"Date column is required for merge but missing for driver {driver}.")

    telemetry = telemetry.sort_values("Date").reset_index(drop=True)
    position = position.sort_values("Date").reset_index(drop=True)

    merged = pd.merge_asof(
        telemetry,
        position,
        on="Date",
        suffixes=("", "_pos"),
        direction="nearest",
        tolerance=pd.Timedelta(milliseconds=100),
    )

    if "SessionTime_pos" in merged.columns and "SessionTime" in merged.columns:
        merged["SessionTime"] = merged["SessionTime"].fillna(merged["SessionTime_pos"])
        merged = merged.drop(columns=["SessionTime_pos"])

    merged["Driver"] = driver
    return merged


def build_replay_dataframe(session) -> pd.DataFrame:
    driver_frames = []
    for driver in DRIVERS:
        logging.info("Extracting telemetry/position for %s", driver)
        driver_frame = build_driver_dataframe(session, driver)
        driver_frames.append(driver_frame)

    replay_df = pd.concat(driver_frames, ignore_index=True)
    replay_df = replay_df.sort_values("Date").reset_index(drop=True)
    return replay_df


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )


def serialize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_payload(row: pd.Series) -> dict:
    payload = {column: serialize_value(row[column]) for column in row.index}
    return payload


def stream_replay(replay_df: pd.DataFrame, producer: KafkaProducer) -> None:
    previous_timestamp = None

    for _, row in replay_df.iterrows():
        current_timestamp = row["Date"]

        if previous_timestamp is not None:
            delta_seconds = (current_timestamp - previous_timestamp).total_seconds()
            if delta_seconds > 0:
                time.sleep(delta_seconds)

        payload = row_to_payload(row)
        producer.send(TOPIC_NAME, value=payload)
        logging.info("Sent event | driver=%s | date=%s | speed=%s", payload.get("Driver"), payload.get("Date"), payload.get("Speed"))

        previous_timestamp = current_timestamp

    producer.flush()


if __name__ == "__main__":
    configure_logging()
    cache_path = configure_cache()
    logging.info("FastF1 cache enabled at: %s", cache_path)

    logging.info("Loading FastF1 session: 2023 Italian Grand Prix - Race")
    race_session = load_session()
    replay_dataframe = build_replay_dataframe(race_session)
    logging.info("Replay dataframe prepared with %d rows", len(replay_dataframe))

    kafka_producer = create_producer()
    logging.info("Kafka producer initialized on localhost:9092, topic=%s", TOPIC_NAME)

    try:
        stream_replay(replay_dataframe, kafka_producer)
    except KeyboardInterrupt:
        logging.info("Streaming interrupted by user")
    finally:
        kafka_producer.close()
        logging.info("Kafka producer closed")
