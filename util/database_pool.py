import sys

import psycopg
import psycopg_pool
from psycopg.types.composite import CompositeInfo, register_composite

from util.data_transformer import re_sql

class DatabasePool(object):
    def __init__(self, config, logger=None):
        self.user = config["user"]
        self.database = config["name"]
        self.password = config["password"]
        self.fetch_rows = config["fetch_rows"]
        self.min_connections = config["min_connections"]

        self.logger = logger

        self.connect()

    def init_app(self, app):
        app.extensions = getattr(app, "extensions", {})
        if "database" not in app.extensions:
            app.extensions["database"] = self

    def connect(self):
        try:
            connection_str = (
                f"postgresql://{self.user}:{self.password}@localhost/{self.database}"
            )
            self.connection_pool = psycopg_pool.ConnectionPool(
                conninfo=connection_str,
                min_size=self.min_connections,
                open=True,
            )
        except psycopg.OperationalError as e:
            if self.logger:
                self.logger.error(f"Could not connect to database:\n{e}")
            else:
                sys.stderr.write(f"Could not connect to database:\n{e}\n")
            raise psycopg.OperationalError(e)

    def register_type(self, type_name):
        with self.connection_pool.connection() as conn:
            info = CompositeInfo.fetch(conn, type_name)
            register_composite(info, conn)

    def terminate(self, verbose=True):
        if verbose:
            if self.logger:
                self.logger.info("Terminating database pool")
            else:
                sys.stdout.write("Terminating database pool\n")
        self.connection_pool.close()

    def sql_return_one(self, sql, parameters=None, custom_types=[]):
        try:
            with self.connection_pool.connection() as conn:
                for type_name in custom_types:
                    info = CompositeInfo.fetch(conn, type_name)
                    register_composite(info, conn)
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters)
                    return cursor.fetchone()
        except Exception as e:
            if self.logger:
                self.logger.error(f"Could not execute SQL statement:\n{re_sql(sql)}")
                self.logger.error(f"Error:\n{e}")
            else:
                sys.stderr.write(f"Could not execute SQL statement:\n{re_sql(sql)}\n")
                sys.stderr.write(f"Error:\n{e}\n")
        return None

    def sql_return_all(self, sql, parameters=None, custom_types=[]):
        try:
            with self.connection_pool.connection() as conn:
                for type_name in custom_types:
                    info = CompositeInfo.fetch(conn, type_name)
                    register_composite(info, conn)
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters)
                    return cursor.fetchall()
        except Exception as e:
            if self.logger:
                self.logger.error(f"Could not execute SQL statement:\n{re_sql(sql)}")
                self.logger.error(f"Error:\n{e}")
            else:
                sys.stderr.write(f"Could not execute SQL statement:\n{re_sql(sql)}\n")
                sys.stderr.write(f"Error:\n{e}\n")
            return None

    def sql_update_table(self, sql, parameters, update=False):
        try:
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters)
                    conn.commit()
                    if update:
                        return cursor.rowcount
        except Exception as e:
            if self.logger:
                self.logger.error(f"Could not execute SQL statement:\n{re_sql(sql)}")
                self.logger.error(f"Error:\n{e}")
            else:
                sys.stderr.write(f"Could not execute SQL statement:\n{re_sql(sql)}\n")
                sys.stderr.write(f"Error:\n{e}\n")

    def sql_execute_many(self, sql, data, template=None, custom_types=[]):
        try:
            with self.connection_pool.connection() as conn:
                for type_name in custom_types:
                    info = CompositeInfo.fetch(conn, type_name)
                    register_composite(info, conn)
                with conn.cursor() as cursor:
                    cursor.executemany(sql, data, template=template)
                    conn.commit()
        except Exception as e:
            if self.logger:
                self.logger.error(f"Could not execute SQL statement:\n{re_sql(sql)}")
                self.logger.error(f"Error:\n{e}")
            else:
                sys.stderr.write(f"Could not execute SQL statement:\n{re_sql(sql)}\n")
                sys.stderr.write(f"Error:\n{e}\n")

    def build_sql(self, sql, parameters):
        try:
            return self.cursor.mogrify(sql, parameters)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Could not execute SQL statement:\n{re_sql(sql)}")
                self.logger.error(f"Error:\n{e}")
            else:
                sys.stderr.write(f"Could not execute SQL statement:\n{re_sql(sql)}\n")
                sys.stderr.write(f"Error:\n{e}\n")
