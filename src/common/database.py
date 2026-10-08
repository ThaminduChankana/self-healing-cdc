"""Database connector abstraction.

Three distinct credentials are used on purpose:
  * reader  — read-only inspection of the source (contract generation, drift discovery)
  * writer  — the *simulated upstream application* (seed data, controlled drift)
  * sandbox — the isolated MySQL where AI-proposed SQL is tested

The AI repair worker only ever receives the sandbox credentials.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import mysql.connector

from src.common.config import get_settings


class DatabaseUnavailableError(Exception):
    pass


class DatabaseConnector(ABC):
    @abstractmethod
    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]: ...

    @abstractmethod
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int: ...

    @abstractmethod
    def close(self) -> None: ...


class MySQLConnector(DatabaseConnector):
    def __init__(self, host: str, port: int, user: str, password: str, database: str | None = None,
                 connect_timeout: int = 5):
        try:
            self._conn = mysql.connector.connect(
                host=host, port=port, user=user, password=password, database=database,
                connection_timeout=connect_timeout, autocommit=True, use_pure=True,
            )
        except mysql.connector.Error as exc:
            raise DatabaseUnavailableError(f"cannot connect to {host}:{port} as {user}: {exc.msg}") from exc

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        cur = self._conn.cursor(dictionary=True)
        try:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
        finally:
            cur.close()

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or ())
            return cur.rowcount
        finally:
            cur.close()

    def close(self) -> None:
        try:
            self._conn.close()
        except mysql.connector.Error:
            pass


@contextmanager
def source_reader() -> Iterator[MySQLConnector]:
    s = get_settings()
    conn = MySQLConnector(s.mysql_host, s.mysql_port, s.mysql_reader_user,
                          s.mysql_reader_password.get_secret_value(), s.mysql_database)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def source_writer() -> Iterator[MySQLConnector]:
    s = get_settings()
    conn = MySQLConnector(s.mysql_host, s.mysql_port, s.mysql_writer_user,
                          s.mysql_writer_password.get_secret_value(), s.mysql_database)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def sandbox_connection() -> Iterator[MySQLConnector]:
    s = get_settings()
    conn = MySQLConnector(s.sandbox_db_host, s.sandbox_db_port, s.sandbox_db_user,
                          s.sandbox_db_password.get_secret_value())
    try:
        yield conn
    finally:
        conn.close()
