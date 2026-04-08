from datetime import timezone

from sqlalchemy import DateTime, String
from sqlalchemy.orm import DeclarativeBase, mapped_column


def dt_to_iso_z(value):
    if value is None:
        return None

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Base(DeclarativeBase):
    pass


class ServiceStatus(Base):
    __tablename__ = "service_status"

    service_name = mapped_column(String(32), primary_key=True)
    status = mapped_column(String(16), nullable=False)
    last_checked = mapped_column(DateTime(timezone=True), nullable=False)
