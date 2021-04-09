#!/usr/bin/env python3

import json
import sqlite3
import data_loader

from datetime import datetime
from pymongo import MongoClient
from test_case import TestCases
from queries import Queries


def serialize_default(obj):
    if isinstance(obj, bytes):
        return ''.join('{:02x}'.format(b) for b in obj)
    elif isinstance(obj, datetime):
        return datetime.strftime(obj, '%Y-%m-%d %H:%M:%S.%f')
    else:
        return obj

def pretty_print_json(obj):
    return json.dumps(obj, indent=2, default=serialize_default)

def main():
    # Input: SQLite3
    conn = sqlite3.connect('../ExampleData/example_data_before_migration.sqlite3')
    conn.row_factory = sqlite3.Row

    # Output: MongoDB
    client = MongoClient('localhost', 27017)
    db = client.db_before_migration

    if db.user.count_documents({}) == 0:
        print('Loading data\n')
        data_loader.populate_database(conn, db)
    else:
        print('Warning: Database already exists; not reloading data\n')

    # Read parameters and reference results
    tc_file = open('../ExampleData/query_results_before_migration.json')
    test_cases = TestCases(tc_file)
    queries = Queries(db, test_cases)
    results, metrics = queries.execute()
    failed = False

    # Write complexity metrics to file
    metrics.to_csv('metrics.csv')

    if len(test_cases) != len(results):
        raise RuntimeError(
            'There are {} queries but {} tests'.format(len(results), len(test_cases))
        )

    for result in results:
        if len(result.violation_indexes) == 0:
            # Success
            print('{0.name} Succeeded in {0.elapsed_time:.4f} s\n----------------'.format(result))
        else:
            # Error
            actual = pretty_print_json(result.actual)
            expected = pretty_print_json(result.expected)

            print(
                '''{result.name} FAILED
Mismatch indexes: {result.violation_indexes}

Actual results: {actual}

Expected results: {expected}
----------------'''.format(result=result, actual=actual, expected=expected)
            )
            failed = True

    if failed:
        raise RuntimeError('Some tests failed')

if __name__ == '__main__':
    main()
