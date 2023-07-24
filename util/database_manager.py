import psycopg2
import psycopg2.extras
import sys

class DatabaseManager(object):
    def __init__(self, db_config, named_cursor=False, logger=None):
        self.db_user = db_config["user"]
        self.db_name = db_config["name"]
        self.db_pass = db_config["password"]
        self.fetch_rows = db_config["fetch_rows"]

        self.named_cursor = named_cursor

        self.logger = logger

        self.connect()

    def connect(self):
        try:
            if self.db_pass:
                self.connection = psycopg2.connect(user=self.db_user, dbname=self.db_name, password=self.db_pass)
            else:
                self.connection = psycopg2.connect(user=self.db_user, dbname=self.db_name)
            if self.named_cursor:
                self.cursor = self.connection.cursor("cursor")
                self.cursor.itersize = self.fetch_rows # Limit number of rows fetched
            else:
                self.cursor = self.connection.cursor()
        except psycopg2.OperationalError as e:
            str_error = str(e).replace("\n", "").replace("\t", " ")
            if self.logger:
                self.logger.error(f"Could not connect to database, error message: {str_error}")
            else:
                sys.stderr.write(f"Could not connect to database, error message: {str_error}\n")
            raise psycopg2.OperationalError(e)

    def register_type(self, type_name):
        psycopg2.extras.register_composite(type_name, self.connection)

    def terminate(self, verbose=True):
        if verbose:
            if self.logger:
                self.logger.info("Terminating database manager")
            else:
                sys.stdout.write("Terminating database manager\n")
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

    def sql_insert_one(self, sql, data):
        try:
            sql = self.cursor.mogrify(sql, data)
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            if self.logger:
                self.logger.error("Could not execute SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")

    def sql_return_one(self, sql, parameters=None):
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

    def sql_return_all(self, sql):
        try:
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

    def sql_update_table(self, sql):
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            return self.cursor.rowcount
        except Exception as e:
            if self.logger:
                self.logger.error("Could not execute SQL statement '" + str(sql) + "', error: " + str(e))
            else:
                sys.stderr.write("Could not execute SQL statement '" + str(sql) + "', error: " + str(e) + "\n")

    def sql_execute_many(self, sql, data, template=None):
        try:
            if template:
                psycopg2.extras.execute_values(self.cursor, sql, data, template=template, page_size=256)
            else:
                psycopg2.extras.execute_values(self.cursor, sql, data, page_size=256)
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
