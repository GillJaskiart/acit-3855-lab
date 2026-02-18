from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, func, Float
from datetime import timezone


def _dt_to_iso_z(dt):
    if dt is None:
        return None
    # If it's naive, assume UTC (better than crashing)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")



class Base(DeclarativeBase):
    pass

class SpeedingViolation(Base):
    __tablename__ = "speeding_violation"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    # batch
    sender_id = mapped_column(String(36), nullable=False)
    location_id = mapped_column(String(10), nullable=False)
    trace_id = mapped_column(String(36), nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)

    # violations object
    reading_timestamp = mapped_column(DateTime, nullable=False)
    speed_kmh = mapped_column(Float, nullable=False)
    speed_limit_kmh = mapped_column(Float, nullable=False)
    direction = mapped_column(String(15), nullable=True)

    date_created = mapped_column(DateTime, nullable=False, default=func.now())

    def to_dict(self):
        return {
            "sender_id": self.sender_id,
            "location_id": self.location_id,
            "trace_id": self.trace_id,
            "speed_kmh": self.speed_kmh,
            "speed_limit_kmh": self.speed_limit_kmh,
            "direction": self.direction,
            "batch_timestamp": _dt_to_iso_z(self.batch_timestamp),
            "reading_timestamp": _dt_to_iso_z(self.reading_timestamp),
        }

class CongestionCount(Base):
    __tablename__ = "congestion_count"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    # batch
    sender_id = mapped_column(String(36), nullable=False)
    location_id = mapped_column(String(10), nullable=False)
    trace_id = mapped_column(String(36), nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)

    # congestion counts object
    reading_timestamp = mapped_column(DateTime, nullable=False)
    vehicles_passing = mapped_column(Integer, nullable=False)
    interval_seconds = mapped_column(Integer, nullable=False)
    direction = mapped_column(String(15), nullable=True)

    date_created = mapped_column(DateTime, nullable=False, default=func.now())

    def to_dict(self):
        return {
            "sender_id": self.sender_id,
            "location_id": self.location_id,
            "trace_id": self.trace_id,
            "vehicles_passing": self.vehicles_passing,
            "interval_seconds": self.interval_seconds,
            "direction": self.direction,
            "batch_timestamp": _dt_to_iso_z(self.batch_timestamp),
            "reading_timestamp": _dt_to_iso_z(self.reading_timestamp),
        }