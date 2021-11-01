#!/usr/bin/env python3

import csv
import enum
import os
import pprint
import re
import sqlite3
import sys
from functools import partialmethod


class PricingField(enum.IntEnum):
    # The value of a member represent column number of a csv file.
    InstanceType = 1
    LinuxOnDemandPrice = 30
    WindowsOnDemandPrice = 36

    def format(self, text):
        return getattr(self, '_{}'.format(self.name), lambda x: x)(text)

    @staticmethod
    def _LinuxOnDemandPrice(text):
        return re.sub('[^0-9.]', '', text.lower())

    _WindowsOnDemandPrice = _LinuxOnDemandPrice


class CSVPriceRecord:
    def __init__(self, rec):
        self.record = rec

    def data(self, field):
        return field.format(self.record[field.value])

    windows_price = partialmethod(data, PricingField.WindowsOnDemandPrice)
    linux_price = partialmethod(data, PricingField.LinuxOnDemandPrice)
    instance_type = partialmethod(data, PricingField.InstanceType)

    def is_linux_platform(self):
        return self.linux_price() is not ''

    def is_windows_platform(self):
        return self.windows_price() is not ''

    @staticmethod
    def region(file_path):
        return os.path.basename(file_path).split('.')[0]


class CSVPricingProcessor:
    NAMESPACE = 'prices'

    def __init__(self, csv_dir):
        self.csv_directory = csv_dir

    def csv_files(self):
        for root, _, files in os.walk(self.csv_directory):
            for csv_file in files:
                yield os.path.join(root, csv_file)

    def extractor(self):
        for f in self.csv_files():
            with open(f) as csv_file:
                reader = csv.reader(csv_file)
                # Skip header
                next(reader)
                for rec in reader:
                    row = CSVPriceRecord(rec)
                    if row.is_linux_platform():
                        yield (self.NAMESPACE, '{}:{}:{}'.format(row.region(f), 'linux', row.instance_type()),
                               row.linux_price())
                    if row.is_windows_platform():
                        yield (self.NAMESPACE, '{}:{}:{}'.format(row.region(f), 'windows', row.instance_type()),
                               row.windows_price())


class MetricsDB:
    def __init__(self, data):
        """
        Metrics db may contains multiple tree like metrics separated by different namespaces.
        There are three main fields:
        namespace - just data domain, like pricing, system_resources etc.
        path - dotted (semicolon in our case) representation of a tree structure. Saying, if there is a tree, like
               parent1
                    |_child1.1
                    |_child1.2
               parent2
                    |_child2.1
               With a dotted notation it can be represented like:
                   parent1:child1.1
                   parent1:child1.2
                   parent2:child2.1
        value - any number value.
        :param data:
        - list of tuples (namespace, dotted_path, value)
        """
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        self.cursor.execute('CREATE TABLE metrics(namespace text, path text, val real)')
        self.cursor.executemany('INSERT INTO metrics VALUES (?, ?, ?)', data)
        self.conn.commit()

    def lookup(self, namespace, query):
        """Lookups for a data within namespace.
        :param namespace: string, which represent actually data domain.
        :param query:  string, which contains wildcards to search through the data. Example,
                       '%' - returns all the data within namespace.
                       '%:m5.large' - returns only data with m5.large keys.
                       'us-east-1:%:m5.large' - returns any data within us-east-1 which are limited to m5.large.
        :returns dict: tree like dictionary.
        """
        self.cursor.execute('SELECT path, val FROM metrics WHERE namespace="{}" AND path like "{}"'.format(
            namespace, query
        ))

        result = {}
        for path, val in self.cursor.fetchall():
            prev = orig = result
            for key in path.split(':'):
                prev = orig
                orig = orig.setdefault(key, {})
            else:
                prev[key] = val
        return result


def app():
    database = MetricsDB(CSVPricingProcessor('./csv').extractor())
    print('PRICING_DATABASE = {}'.format(
        pprint.pformat(database.lookup(CSVPricingProcessor.NAMESPACE, '%'), width=40, compact=True)
    ))
    return 0


if __name__ == '__main__':
    sys.exit(app())
