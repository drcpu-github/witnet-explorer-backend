import json
import os
import sqlite3
import sys

import psycopg


class MockDatabase(object):
    def __init__(self):
        self.connection = sqlite3.connect(
            f"{os.path.realpath(os.path.dirname(__file__))}/data/database.sqlite3"
        )

    def init_app(self, app):
        app.extensions = getattr(app, "extensions", {})
        if "database" not in app.extensions:
            app.extensions["database"] = self

    def transform_sql(self, sql, parameters):
        # replace %s-style arguments with ?-style
        if "%s" in sql:
            sql = sql.replace("%s", "?")
        # sqlite3 does not have a boolean type
        # select where statements have to select based on ='true' / ='false'
        if "=true" in sql:
            sql = sql.replace("=true", "='True'")
        if "=false" in sql:
            sql = sql.replace("=false", "='False'")
        # commit is a reserved keyword, replace it with 'commit'
        if "commit," in sql:
            sql = sql.replace("commit,", '"commit",')
        # replace 'in-array' where-clause: @> ARRAY[?]::CHAR(42)[]
        if "@> ARRAY[?]::CHAR(42)[]" in sql:
            sql = sql.replace("@> ARRAY[?]::CHAR(42)[]", "LIKE ?")
            new_parameters = []
            for parameter in parameters:
                if parameter.startswith("wit1"):
                    new_parameters.append("%" + parameter + "%")
                else:
                    new_parameters.append(parameter)
            parameters = new_parameters
        # replace 'not-in-array' where-clause: NOT (? = ANY(...))
        pattern = "NOT (? = ANY("
        if pattern in sql:
            sql_lines = [line.strip() for line in sql.splitlines()]
            for i, line in enumerate(sql_lines):
                if pattern in line:
                    column = line.replace(pattern, "")
                    column = column.replace(")", "")
                    column = column.strip()
                    sql_lines[i] = f"{column} NOT LIKE ?"
                    break
            sql = " ".join(sql_lines)
        # replace the ANY operator
        if "= ANY(?)" in sql:
            if len(parameters) > 1:
                sys.stderr.write(
                    "Cannot use the ANY operator in combination with multiple parameters lists."
                )
                sys.exit(1)
            sql = sql.replace("= ANY(?)", f"IN ({','.join('?' * len(parameters[0]))})")
            parameters = parameters[0]
        # instead of returning all wips, return only one for the sake of testing
        if "wips" in sql:
            sql = sql.replace(
                "tapi_bit IS NOT NULL", "tapi_bit IS NOT NULL AND (id=7 OR id=13)"
            )
        return sql, parameters

    def transform_parameters(self, parameters):
        if parameters:
            for idx, parameter in enumerate(parameters):
                if isinstance(parameter, bytearray):
                    parameters[idx] = f"\\x{parameter.hex()}"
        return parameters

    def transform_data(self, sql, data, multi_row=False):
        # sqlite3 features a very limited set of data types:
        #   1) It does not have a bytea type:
        #       Transform the returned hex string to a bytearray for testing purposes
        #       this way no changes have to be made in the actual application
        #   2) It does not have an array type:
        #       Transform data surrounded by {} to an array
        #   3) It does not have a bool type:
        #       Transform data with the value of true or false to a boolean
        if not multi_row:
            data = [data]
        for row in range(len(data)):
            if data[row] is None:
                continue
            data[row] = [d for d in data[row]]
            for column in range(len(data[row])):
                value = data[row][column]
                if "network_stats" in sql or (
                    "tapi_json" in sql and "tapi_bit" not in sql
                ):
                    if value is None:
                        continue
                    try:
                        data[row][column] = int(value)
                        continue
                    except ValueError:
                        pass
                    try:
                        data[row][column] = json.loads(value)
                        continue
                    except json.decoder.JSONDecodeError:
                        pass
                elif isinstance(value, str) and value.startswith("\\x"):
                    data[row][column] = bytearray.fromhex(value[2:])
                elif (
                    isinstance(value, str)
                    and value.startswith("[")
                    and value.endswith("]")
                ):
                    try:
                        data[row][column] = [int(v) for v in value[1:-1].split(",")]
                    except ValueError:
                        if value == "[]":
                            data[row][column] = []
                        else:
                            if "filter" in value:
                                data[row][column] = [
                                    v.strip() for v in value[1:-1].split("),")
                                ]
                                for i in range(len(data[row][column])):
                                    f_type, f_args = data[row][column][i].split(",")
                                    data[row][column][i] = [
                                        int(f_type[7:]),
                                        bytearray.fromhex(f_args.strip()[2:-1]),
                                    ]
                                continue
                            else:
                                data[row][column] = [
                                    v.strip() for v in value[1:-1].split(",")
                                ]
                            # Transform back to an enum array
                            if (
                                "HTTP-GET" in data[row][column]
                                or "HTTP-POST" in data[row][column]
                                or "RNG" in data[row][column]
                            ):
                                data[row][column] = (
                                    "{" + ",".join(data[row][column]) + "}"
                                )
                                continue
                            # Transform elements in array if needed
                            for i in range(len(data[row][column])):
                                entry = data[row][column][i]
                                # Transform data to a UTXO if needed
                                if entry.startswith("\\x") and ":" in entry:
                                    hash_value, idx = entry.split(":")
                                    data[row][column][i] = (
                                        bytearray.fromhex(hash_value[2:]),
                                        int(idx),
                                    )
                                # Transaction hash
                                elif entry.startswith("\\x"):
                                    data[row][column][i] = bytearray.fromhex(entry[2:])
                                # 2-dimensional array
                                elif entry.startswith("[") and entry.endswith("]"):
                                    data[row][column][i] = [
                                        e.strip().replace("'", "")
                                        for e in entry[1:-1].split(",")
                                    ]

                elif isinstance(value, str) and value == "True":
                    data[row][column] = True
                elif isinstance(value, str) and value == "False":
                    data[row][column] = False
        if not multi_row:
            data = data[0]
        return data

    def composable_as_string(self, composable, encoding="utf-8"):
        if isinstance(composable, psycopg.sql.Composed):
            return "".join([self.composable_as_string(x, encoding) for x in composable])
        elif isinstance(composable, psycopg.sql.SQL):
            return composable.as_string(None)
        else:
            return " ,".join(composable._obj)

    def sql_return_one(self, sql, parameters=None, custom_types=None):
        # custom_types is ingnored, this is just a non-mock compatibility parameter
        if isinstance(sql, psycopg.sql.Composed):
            sql = self.composable_as_string(sql)
        sql, parameters = self.transform_sql(sql, parameters)
        parameters = self.transform_parameters(parameters)
        cursor = self.connection.cursor()
        if parameters is None:
            data = cursor.execute(sql).fetchone()
        else:
            data = cursor.execute(sql, parameters).fetchone()
        return self.transform_data(sql, data)

    def sql_return_all(self, sql, parameters=None, custom_types=None):
        # custom_types is ingnored, this is just a non-mock compatibility parameter
        if isinstance(sql, psycopg.sql.Composed):
            sql = self.composable_as_string(sql)
        sql, parameters = self.transform_sql(sql, parameters)
        parameters = self.transform_parameters(parameters)
        cursor = self.connection.cursor()
        if parameters is None:
            data = cursor.execute(sql).fetchall()
        else:
            data = cursor.execute(sql, parameters).fetchall()
        return self.transform_data(sql, data, multi_row=True)
