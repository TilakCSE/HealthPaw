from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Dog(Base):
    __tablename__ = "dogs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    breed: Mapped[str | None] = mapped_column(String(100), nullable=True)
    life_stage: Mapped[str | None] = mapped_column(String(50), nullable=True)
    body_weight_kg: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
    )

    devices: Mapped[list["Device"]] = relationship(
        back_populates="dog"
    )


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    device_id: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
    )
    dog_id: Mapped[int | None] = mapped_column(
        ForeignKey("dogs.id"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(30),
        default="active",
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
    )

    dog: Mapped["Dog | None"] = relationship(
        back_populates="devices"
    )

    telemetry: Mapped[list["Telemetry"]] = relationship(
        back_populates="device"
    )


class Telemetry(Base):
    __tablename__ = "telemetry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    device_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("devices.device_id"),
        nullable=False,
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
    )

    current_weight_g: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    gate_status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
    )

    device: Mapped["Device"] = relationship(
        back_populates="telemetry"
    )