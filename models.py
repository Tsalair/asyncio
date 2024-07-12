import os

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy import JSON, DateTime, String, Integer

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase, AsyncAttrs):
    pass

class SwapiPeople(Base):

    __tablename__ = 'swapi_people'

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=False, nullable=False)
    birth_year: Mapped[str] = mapped_column(unique=False, nullable=False)
    eye_color: Mapped[str] = mapped_column(unique=False, nullable=False)
    gender: Mapped[str] = mapped_column(unique=False, nullable=False)
    hair_color: Mapped[str] = mapped_column(unique=False, nullable=False)
    height: Mapped[str] = mapped_column(unique=False, nullable=False)
    homeworld: Mapped[str] = mapped_column(unique=False, nullable=False)
    mass: Mapped[str] = mapped_column(unique=False, nullable=False)
    skin_color: Mapped[str] = mapped_column(unique=False, nullable=False)
    films: Mapped[str] = mapped_column(unique=False, nullable=False)
    species: Mapped[str] = mapped_column(unique=False, nullable=False)
    starships: Mapped[str] = mapped_column(unique=False, nullable=False)
    vehicles: Mapped[str] = mapped_column(unique=False, nullable=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


