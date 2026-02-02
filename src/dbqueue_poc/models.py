import enum
import uuid
from datetime import datetime, timezone
from typing import Callable, Generic, List, Optional, TypeVar

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    TypeDecorator,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy_utils import UUIDType

from dbqueue_poc.schemas import JobStatus, RunStatus
from dbqueue_poc.utils.common import get_current_datetime


class NaiveDateTime(TypeDecorator):
    """
    A custom type decorator that ensures datetime objects are offset-naive when stored in the database
    and offset-aware with UTC timezone when loaded from the database.
    This is because we use datetimes in UTC everywhere, and
    some databases (e.g. Postgres) throw an error if the timezone is set.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None and value.tzinfo is not None:
            return value.replace(tzinfo=None)
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc)


E = TypeVar("E", bound=enum.Enum)


class EnumAsString(TypeDecorator, Generic[E]):
    """
    A custom type decorator that stores enums as strings in the DB.
    """

    impl = String
    cache_ok = True

    def __init__(
        self,
        enum_class: type[E],
        *args,
        fallback_deserializer: Optional[Callable[[str], E]] = None,
        **kwargs,
    ):
        """
        Args:
            enum_class: The enum class to be stored.
            fallback_deserializer: An optional function used when the string
                from the DB does not match any enum member name. If not
                provided, an exception will be raised in such cases.
        """
        self.enum_class = enum_class
        self.fallback_deserializer = fallback_deserializer
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value: Optional[E], dialect) -> Optional[str]:
        if value is None:
            return None
        return value.name

    def process_result_value(self, value: Optional[str], dialect) -> Optional[E]:
        if value is None:
            return None
        if value not in self.enum_class.__members__ and self.fallback_deserializer is not None:
            return self.fallback_deserializer(value)
        return self.enum_class[value]


constraint_naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class BaseModel(DeclarativeBase):
    metadata = MetaData(naming_convention=constraint_naming_convention)


class RunModel(BaseModel):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )

    name: Mapped[str] = mapped_column(String(100))
    submitted_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    last_processed_at: Mapped[datetime] = mapped_column(
        NaiveDateTime, default=get_current_datetime
    )
    status: Mapped[RunStatus] = mapped_column(EnumAsString(RunStatus, 100), index=True)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    lock_expires_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)
    lock_token: Mapped[Optional[uuid.UUID]] = mapped_column(UUIDType(binary=False))

    run_spec: Mapped[str] = mapped_column(Text)
    priority: Mapped[int] = mapped_column(Integer, default=0)

    jobs: Mapped[List["JobModel"]] = relationship(back_populates="run")

    __table_args__ = (Index("ix_submitted_at_id", submitted_at.desc(), id),)


class JobModel(BaseModel):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), index=True
    )
    run: Mapped["RunModel"] = relationship()

    name: Mapped[str] = mapped_column(String(100))
    submitted_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    last_processed_at: Mapped[datetime] = mapped_column(
        NaiveDateTime, default=get_current_datetime
    )
    status: Mapped[JobStatus] = mapped_column(EnumAsString(JobStatus, 100), index=True)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)


class EventModel(BaseModel):
    __tablename__ = "events"

    id: Mapped[uuid.UUID] = mapped_column(UUIDType(binary=False), primary_key=True)
    message: Mapped[str] = mapped_column(Text)
    recorded_at: Mapped[datetime] = mapped_column(
        NaiveDateTime, default=get_current_datetime, index=True
    )
