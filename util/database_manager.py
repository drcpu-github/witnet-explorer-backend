import psycopg
from psycopg.types.composite import CompositeInfo, register_composite
import sys

class DatabaseManager(object):
    def __init__(self, db_config, named_cursor=False, logger=None, custom_types=[]):
        self.db_user = db_config["user"]
        self.db_name = db_config["name"]
        self.db_pass = db_config["password"]
        self.fetch_rows = db_config["fetch_rows"]

        self.named_cursor = named_cursor

        self.logger = logger

        self.connect(custom_types)

    def connect(self, custom_types):
        try:
            if self.db_pass:
                self.connection = psycopg.connect(user=self.db_user, dbname=self.db_name, password=self.db_pass)
            else:
                self.connection = psycopg.connect(user=self.db_user, dbname=self.db_name)

            for ct in custom_types:
                self.register_type(ct)

            if self.named_cursor:
                self.cursor = self.connection.cursor("cursor")
                self.cursor.itersize = self.fetch_rows # Limit number of rows fetched
            else:
                self.cursor = self.connection.cursor()
        except psycopg.OperationalError as e:
            str_error = str(e).replace("\n", "").replace("\t", " ")
            if self.logger:
                self.logger.error(f"Could not connect to database, error message: {str_error}")
            else:
                sys.stderr.write(f"Could not connect to database, error message: {str_error}\n")
            raise psycopg.OperationalError(e)

    def register_type(self, type_name):
        info = CompositeInfo.fetch(self.connection, type_name)
        register_composite(info, self.connection)

    def terminate(self, verbose=False, commit=False):
        if verbose:
            if self.logger:
                self.logger.info("Terminating database manager")
            else:
                sys.stdout.write("Terminating database manager\n")
        if commit:
            self.connection.commit()
        # Cannot close a named cursor once commit has been called
        if not self.named_cursor:
            self.cursor.close()
        self.connection.close()

    def reset_cursor(self):
        self.cursor.close()
        if self.named_cursor:
            self.cursor = self.connection.cursor("cursor")
            self.cursor.itersize = 1000 # Limit number of rows fetched
        else:
            self.cursor = self.connection.cursor()

    def sql_insert_one(self, sql, parameters=None):
        try:
            if parameters:
                self.cursor.execute(sql, parameters)
            else:
                self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            if self.logger:
                self.logger.error("Could not execute SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")

    # Note: custom types is unused here, but the option exists to have the same calling convention as when using a database pool connection
    def sql_return_one(self, sql, parameters=None, custom_types=[]):
        try:
            if parameters:
                self.cursor.execute(sql, parameters)
            else:
                self.cursor.execute(sql)
            return self.cursor.fetchone()
        except Exception as e:
            if self.logger:
                self.logger.error("Could not execute SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")
            return None

    # Note: custom types is unused here, but the option exists to have the same calling convention as when using a database pool connection
    def sql_return_all(self, sql, parameters=None, custom_types=[]):
        try:
            if parameters:
                self.cursor.execute(sql, parameters)
            else:
                self.cursor.execute(sql)
            if self.named_cursor:
                return self.cursor
            else:
                return self.cursor.fetchall()
        except Exception as e:
            if self.logger:
                self.logger.error("Could not execute SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")
            return None

    def sql_update_table(self, sql, parameters=None):
        try:
            self.cursor.execute(sql, parameters)
            self.connection.commit()
            return self.cursor.rowcount
        except Exception as e:
            if self.logger:
                self.logger.error("Could not execute SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")

    # Note: custom types is unused here, but the option exists to have the same calling convention as when using a database pool connection
    def sql_execute_many(self, sql, data, custom_types=[]):
        try:
            self.cursor.executemany(sql, data)
            self.connection.commit()
        except Exception as e:
            if self.logger:
                self.logger.error("Could not execute SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")

    def build_sql(self, sql, values):
        try:
            return self.cursor.mogrify(sql, values)
        except Exception as e:
            if self.logger:
                self.logger.error("Could not create SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")
