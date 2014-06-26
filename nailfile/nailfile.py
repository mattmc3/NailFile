import os
import sqlite3
from collections import OrderedDict

#TODO Copy memory db to file system
#TODO: Dump sqlite database to .sql file


class PreparedStatement:
    """
    Wraper for a SQL statement and its parameters
    """
    @staticmethod
    def standardize_params(params):
        if params is not None:
            if not hasattr(params, '__iter__') or isinstance(params, str):
                result = (params,)
            else:
                result = tuple(params)
        else:
            result = ()
        return result

    def __init__(self, sql, params=None):
        self.sql = sql.strip()
        self.params = PreparedStatement.standardize_params(params)

    def __str__(self):
        result = self.sql + os.linesep
        if len(self.params) > 0:
            result += "-- params: (" + ", ".join([self._to_sql_value(x) for x in self.params]) + ")"
        return result

    def _to_sql_value(self, value):
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float, complex)):
            return str(value)
        else:
            return "'" + str(value).replace("'", "\'") + "'"


def to_html(datarows):
    html = []
    html.append("<table>")
    header = False
    for row in datarows:
        if not header:
            tr = "<tr>" + "".join(["<th>{0}</th>".format(_escape_html(f)) for f in row.fields]) + "</tr>"
            html.append(tr)
            header = True
        tr = "<tr>" + "".join(["<td>{0}</td>".format(_escape_html(v)) for v in row.values]) + "</tr>"
        html.append(tr)
    html.append("</table>")
    return os.linesep.join(html)


def _escape_html(value):
    if value:
        result = str(value)
        result = result.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace(" ", "&nbsp;")
    else:
        result = "<em>&lt;NULL&gt;</em>"
    return result


class DataRow():
    """
    A convenience class for representing a resulting row from a
    database query. It acts both as an OrderedDict and as an
    object with properties named for the columns in the result set
    """
    def __init__(self):
        self._attrs = OrderedDict()

    @property
    def fields(self):
        return list(self._attrs.keys())

    @property
    def values(self):
        return list(self._attrs.values())

    def __str__(self):
        return "{" + ", ".join(["{0}: {1}".format(repr(k), repr(v)) for k, v in self._attrs.items()]) + "}" + \
               os.linesep

    def __repr__(self):
        return self.__str__()

    def __len__(self):
        return self.__dict__.__len__()

    def __getitem__(self, key):
        return self.__dict__.__getitem__(key)

    def __setitem__(self, key, value):
        self.__dict__.__setitem__(key, value)
        if key != '_attrs':
            self._attrs[key] = value

    def __delitem__(self, key):
        self.__dict__.__delitem__(key)
        del self._attrs[key]

    def __setattr__(self, name, value):
        self.__dict__[name] = value
        if name != '_attrs':
            self._attrs[name] = value

    def __iter__(self):
        return self.__dict__.__iter__()


class DataStore():
    """
    Convenience class for interacting with SQLite
    """
    def __init__(self, connection=None):
        if connection is None:
            connection = sqlite3.connect(':memory:')
            connection.row_factory = DataStore.datarow_factory
        self._conn = connection

    @staticmethod
    def datarow_factory(cursor, row):
        d = DataRow()
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def iterquery(self, sql, params=()):
        stmt = PreparedStatement(sql, params)
        cur = self._conn.cursor()
        for row in cur.execute(stmt.sql, stmt.params):
            yield row

    def execute(self, sql, params=()):
        cur = self._execute(sql, params)
        return cur.rowcount

    def fetchall(self, sql, params=()):
        return list(self.iterquery(sql, params))

    def fetchone(self, sql, params=()):
        cur = self._execute(sql, params)
        result = cur.fetchone()
        return result

    def scalar(self, sql, params=()):
        result = self.fetchone(sql, params)
        if result is None or len(result) == 0:
            return None
        else:
            return result.values[0]

    def commit(self):
        self._conn.commit()

    def table_schema(self):
        sql = "select name as TABLE_NAME, 'BASE TABLE' as TABLE_TYPE from sqlite_master where type='table'"
        return self.fetchall(sql)

    def column_schema(self, table_name):
        result = []
        for row in self.fetchall("PRAGMA table_info('{0}')".format(table_name)):
            schema_row = DataRow()
            schema_row.COLUMN_NAME = row['name']
            schema_row.DATA_TYPE = row['type']
            if row['notnull'] == '0':
                schema_row.IS_NULLABLE = "NO"
            else:
                schema_row.IS_NULLABLE = "YES"
            if row['dflt_value'] is None:
                schema_row.COLUMN_DEFAULT = ""
            else:
                schema_row.COLUMN_DEFAULT = row['dflt_value']
            result.append(schema_row)
        return result

    def iterdump(self):
        for line in self._conn.iterdump():
            yield line

    def dump(self, sqlfile):
        with open(sqlfile, 'w') as outfile:
            for line in self._conn.iterdump():
                outfile.write(line)
                outfile.write(os.linesep)

    def _execute(self, sql, params):
        stmt = PreparedStatement(sql, params)
        #print(stmt)
        cur = self._conn.cursor()
        cur.execute(stmt.sql, stmt.params)
        return cur


class DataLoader():
    CHUNK_SIZE = 10000

    def __init__(self):
        pass

    def load(self, reader, connection=None, auto_number_field="row_id", record_number_field="line_num"):
        if connection is None:
            connection = sqlite3.connect(':memory:')
            connection.row_factory = DataStore.datarow_factory

        tables = {}
        rownum = -1
        cur = connection.cursor()

        for row in reader.read():
            rownum += 1
            tbl = reader.table_name
            fields = reader.fields
            num_fields = len(fields)
            num_values = len(row)

            # create the table if we have not encountered it yet
            if not tables.get(tbl):
                tables[tbl] = num_fields
                self._create_table(reader, cur, tbl, auto_number_field, record_number_field)

            # add any new columns
            prior_field_cnt = tables[tbl]
            if prior_field_cnt != num_fields:
                for field in fields[prior_field_cnt:]:
                    alter = "alter table {0} add column {1} {2}".format(tbl, field.name, field.datatype)
                    cur.execute(alter)
                tables[tbl] = num_fields

            field_names = [f.name for f in fields[0:num_values]]
            params = []
            if record_number_field:
                field_names.insert(0, record_number_field)
                params.insert(0, reader.line_number)
            insert = "insert into {0} ({1}) values ({2})".format(reader.table_name,
                                                                 ", ".join(field_names),
                                                                 ", ".join(["?"] * len(field_names)))
            num_values = len(row)
            for i in range(0, num_values):
                if i < num_values:
                    params.append(row[i])
                else:
                    params.append(None)

            if rownum % self.CHUNK_SIZE == 0:
                connection.commit()

            cur.execute(insert, tuple(params))
            rownum += 1

        #cur.execute("COMMIT")
        return DataStore(connection)

    def _create_table(self, reader, cursor, table_name, auto_number_field, record_number_field):
        fields = []
        if auto_number_field is not None:
            fields.append("{0} integer primary key".format(auto_number_field))
        if record_number_field is not None:
            fields.append("{0} integer".format(record_number_field))
        for field in reader.fields:
            fields.append("{0} {1}".format(field.name, field.datatype))
        sql = "create table if not exists {0} ({1})".format(table_name, ", ".join(fields))
        cursor.execute(sql)


if __name__ == '__main__':
    pass
    #TODO make command line interface
