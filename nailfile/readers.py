import csv
import re
import itertools

#TODO: Add multi-record reader
#TODO: Add data conversion handler


class DataField():
    def __init__(self, name, datatype="varchar(255)", coerce_field_name=True):
        new_name = self._coerce_name(name)
        if not coerce_field_name and name != new_name:
            raise ValueError("Unacceptable field name: {0}".format(name))
        self.original_name = name
        self.name = new_name
        self.datatype = datatype

    def _coerce_name(self, fieldname):
        new_name = fieldname

        # handle number sign
        new_name = new_name.replace('#', '_num_')
        # handle percent sign
        new_name = new_name.replace('%', '_pct_')
        # handle non-alpha numeric characters
        new_name = re.sub(r'[^A-Za-z0-9_]', '_', new_name)
        # handle double underscores
        new_name = re.sub(r'_+', '_', new_name)
        # handle leading and trailing underscores
        new_name = re.sub(r'^_|_$', '', new_name)

        return new_name


class DataReader():
    """
    A base class for wrapping the details of reading files in a
    specific format
    """
    def __init__(self):
        self._table_name = "tbl"
        self._line_number = 0
        self._fields = []

    @property
    def table_name(self):
        """
        After each call to read(), this should contain the name of
        the table the data belongs to
        """
        return self._table_name

    @property
    def line_number(self):
        """
        After each call to read(), this should contain the new
        line number of the data returned
        """
        return self._line_number

    @property
    def fields(self):
        """
        After each call to read(), this should contain the fields schema
        for the data elements returned
        """
        return tuple(self._fields)

    def read(self):
        raise NotImplemented("Please implement this method")

    def _set_fields_by_name(self, field_names):
        self._fields = []
        for field_name in field_names:
            self._fields.append(DataField(field_name))

    def _add_field_by_name(self, field_name):
        self._fields.append(DataField(field_name))

    def _ensure_min_defined_fields(self, minimum_fields):
        """
        Helper method for classes that inherit from DataReader to ensure that
        fields are defined to the minimum required number
        """
        for i in range(len(self._fields), minimum_fields):
            self._fields.append(DataField("unnamed_field{0:03d}".format(i + 1)))

    def _format_value(self, value):
        if not value:
            return None
        return value


class DataReaderWrapper(DataReader):
    """
    A data reader that wraps another data reader for the purpose
    of allowing overrides. Both is-a and has-a for DataReader.
    """
    def __init__(self, reader):
        super(DataReaderWrapper, self).__init__()
        self._reader = reader

    @property
    def table_name(self):
        return self._reader.table_name

    @property
    def line_number(self):
        return self._reader.line_number

    @property
    def fields(self):
        return self._reader.fields

    def read(self):
        for row in self._reader.read():
            yield row


class DataReaderLimiter(DataReaderWrapper):
    """
    A data reader that limits the number of rows returned
    """
    def __init__(self, reader, limit):
        super(DataReaderLimiter, self).__init__(reader)
        self._limit = limit

    def read(self):
        for item in itertools.islice(self._reader.read(), self._limit):
            yield item


class CollectionReader(DataReader):
    """
    A data reader for collections
    """
    def __init__(self, fn_get_data, table_name="tbl", headers=True):
        super(CollectionReader, self).__init__()

        self._table_name = table_name
        self.headers = headers
        self.fn_get_data = fn_get_data

        header = self._read_first()
        if headers:
            self._set_fields_by_name(header)
        else:
            self._ensure_min_defined_fields(len(header))

    def read(self):
        self._line_number = 0
        for row in self.fn_get_data():
            self._line_number += 1
            if self._line_number > 1 or not self.headers:
                self._ensure_min_defined_fields(len(row))
                result = tuple(self._format_value(v) for v in row)
                yield result

    def _read_first(self):
        for line in self.fn_get_data():
            return line


class CsvReader(CollectionReader):
    """
    A data reader for csv files
    """
    def __init__(self, filepath, table_name="tbl", headers=True, delimiter=","):
        self.filepath = filepath
        self.delimiter = delimiter
        super(CsvReader, self).__init__(self._read_file, table_name, headers)

    def _read_file(self):
        with open(self.filepath) as infile:
            r = csv.reader(infile, delimiter=self.delimiter)
            for row in r:
                yield row


class FixedWidthReader(DataReader):
    """
    A data reader for fixed-width data
    """
    def __init__(self, fn_get_data, widths, field_names=(), table_name="tbl",
                 remainder_field_name='remainder', strip_values=True):
        super(FixedWidthReader, self).__init__()

        # verify widths provided
        if len(widths) == 0:
            raise ValueError("No field widths provided")
        for w in (w for w in widths if not isinstance(w, int) or w < 0):
            raise ValueError("Invalid width provided: {0}".format(w))

        # verify that there were not more field names provided than widths
        if len(widths) < len(field_names):
            raise ValueError("Too many field names for the number widths specified")

        self._table_name = table_name
        self._widths = tuple(widths)
        self._fn_get_data = fn_get_data
        self._remainder_field_name = remainder_field_name
        self.strip_values = strip_values

        self._fields = []
        for i in range(0, len(widths)):
            width = widths[i]
            field_name = field_names[i] if i < len(field_names) else "unnamed_field{0:3d}".format(i)
            datatype = "varchar({0})".format(str(width) if width > 0 else "1")
            self._fields.append(DataField(field_name, datatype))
        if self._remainder_field_name:
            self._fields.append(DataField(self._remainder_field_name, "text"))

    def read(self):
        self._line_number = 0
        for line in self._fn_get_data():
            result = []
            pos = 0
            for w in self._widths:
                result.append(self._format_value(line[pos:pos+w]))
                pos += w
            if self._remainder_field_name:
                result.append(self._format_value(line[pos:]))
            yield tuple(result)

    def _format_value(self, value):
        if value is not None:
            if self.strip_values:
                value = value.strip()
            if len(value) == 0:
                return None
        return value
