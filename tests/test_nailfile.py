import unittest
from nailfile import readers
from nailfile import nailfile

#TODO: Test dump()


class NailFileTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def get_list_data(self):
        data = [
            ['person_num', 'name', 'gender', 'dob', 'relationship'],
            [1, 'Cliff Huxtable', 'M', '19370712', 'dad'],
            [2, 'Clair Huxtable', 'F', '19480619', 'mom'],
            [3, 'Sondra Huxtable Tibideaux', 'F', None, 'daughter'],
            [4, 'Denise Huxtable Kendall', 'F', None, 'daughter'],
            [5, 'Theo Huxtable', 'M', None, 'son'],
            [6, 'Vanessa Huxtable', 'F', None, 'daughter'],
            [7, 'Rudy Huxtable', 'F', None, 'daughter'],
        ]
        return data

    def get_fixed_width_data(self, ragged_right=True):
        data = self.get_list_data()
        widths = (5, 30, 1, 8, 20)
        result = []
        for row in data[1:]:
            record = ""
            for i in range(len(widths)):
                width = widths[i]
                value = row[i]
                record += ("" if not value else str(value)).ljust(width)
            if ragged_right:
                record = record.rstrip()
            result.append(record)
        return result

    def load_fixed_width_data(self):
        fn_generator = self.get_fixed_width_data
        reader = readers.FixedWidthReader(fn_generator, (5, 30, 1, 8, 20),
                                              ('person_num', 'name', 'gender', 'dob', 'relationship'))
        loader = nailfile.DataLoader()
        ds = loader.load(reader)
        return ds

    def test_triangle_data(self):
        data = [
            (1,),
            (2, 'b'),
            (3, 'c', 'C'),
            (4, 'd', 'D'),
            (5, 'e'),
            (6,)
        ]
        fn = lambda: (x for x in data)
        reader = readers.CollectionReader(fn, headers=False)
        loader = nailfile.DataLoader()
        ds = loader.load(reader)
        rows = ds.fetchall('select * from tbl')
        print(rows)

    def load_collection_data(self, collection=None, exclude_extra_fields=False):
        if not collection:
            collection = self.get_list_data()
        fn_get_list_data = lambda: (c for c in collection)
        reader = readers.CollectionReader(fn_get_list_data)
        loader = nailfile.DataLoader()
        if exclude_extra_fields:
            ds = loader.load(reader, auto_number_field=None, record_number_field=None)
        else:
            ds = loader.load(reader)
        return ds

    def test(self):
        self.assertEqual(1 + 1, 2)

    def test_table_schema(self):
        ds = self.load_collection_data()
        tables = ds.table_schema()
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0].TABLE_NAME, 'tbl')

    def test_column_schema(self):
        ds = self.load_collection_data(exclude_extra_fields=True)
        columns = ds.column_schema('tbl')
        self.assertEqual(len(columns), 5)
        self.assertEqual(columns[0].COLUMN_NAME, 'person_num')
        self.assertEqual(columns[1].COLUMN_NAME, 'name')
        self.assertEqual(columns[2].COLUMN_NAME, 'gender')
        self.assertEqual(columns[3].COLUMN_NAME, 'dob')
        self.assertEqual(columns[4].COLUMN_NAME, 'relationship')

    def test_collection_loads_seven_huxtables(self):
        ds = self.load_collection_data()
        count = ds.scalar('select count(*) from tbl')
        self.assertEqual(count, 7)

    def test_load_with_extra_fields_added_during_read(self):
        huxtables = self.get_list_data()
        huxtables.append([8, 'Olivia', 'F', None, 'step granddaughter', 'Jump the shark!'],)
        ds = self.load_collection_data(huxtables, exclude_extra_fields=True)
        rows = ds.fetchall('select * from tbl where name = ?', params='Olivia')
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0].fields), 6)
        self.assertEqual(rows[0].fields[5], 'unnamed_field006')

        columns = ds.column_schema('tbl')
        self.assertEqual(len(columns), 6)
        self.assertEqual(columns[5].COLUMN_NAME, 'unnamed_field006')

    def test_fixed_width_loads_seven_huxtables(self):
        ds = self.load_fixed_width_data()
        count = ds.scalar('select count(*) from tbl')
        self.assertEqual(count, 7)

    def test_fixed_width_loads_correct_data(self):
        ds = self.load_fixed_width_data()
        guys = ds.fetchall('select * from tbl where gender collate nocase = ? order by name desc', params=('m',))
        self.assertEqual(len(guys), 2)
        self.assertTrue(guys[0].name.startswith('Theo'))
        self.assertTrue(guys[1].name.startswith('Cliff'))
