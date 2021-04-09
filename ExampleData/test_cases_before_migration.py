#!/usr/bin/env python3

import time
import sqlite3
import json
import subprocess
import pandas as pd
from pandas import DataFrame


queries = {
    'continents': '''
        SELECT * FROM region
        WHERE parent_id IS NULL
        ORDER BY id
    ''',

    # Recursive, hierarchical query
    'siblings_and_parents': {
        'sql':
        '''
        WITH RECURSIVE level AS (
                SELECT r2.*
                FROM region AS r1 INNER JOIN region AS r2
                ON r2.parent_id = r1.parent_id
                WHERE r1.id = ?
            UNION
                SELECT r2.*
                FROM region AS r1
                INNER JOIN level ON level.parent_id = r1.id
                INNER JOIN region AS r2 ON r2.parent_id = r1.parent_id OR r2.parent_id IS NULL
        )
        SELECT * FROM level
        ORDER BY level.parent_id DESC, level.id
        ''',
        'params': (348,)
    },

    'no_login_users': '''
        SELECT user.id AS user_id
        FROM user LEFT JOIN session
        ON user.id = session.user_id
        WHERE session.id IS NULL
        ORDER BY user.id
    ''',

    # This is tricky because we can't just use an INNER JOIN,
    # since it would omit 0-session users from the results.
    'num_valid_sessions': '''
        SELECT
            user.id AS user_id,
            SUM(session.logout_public_key IS NULL AND session.id IS NOT NULL) AS valid_sessions
        FROM user LEFT JOIN session
        ON user.id = session.user_id
        GROUP BY user.id
        ORDER BY user.id
    ''',

    'multi_profile_users': '''
        SELECT user_id, COUNT(id) AS profile_count
        FROM profile
        GROUP BY user_id
        HAVING profile_count > 1
        ORDER BY user_id
    ''',

    'owned_real_estate_region_count': '''
        SELECT user.id AS user_id,
               COUNT(DISTINCT real_estate.region_id) AS region_count
        FROM user
        LEFT JOIN real_estate
        ON real_estate.owner_id = user.id
        GROUP BY user.id
        ORDER BY user.id
    ''',

    'profile_counts_by_non_google_user': '''
        SELECT
            user.id AS user_id,
            COUNT(profile.facebook_id) AS fb_profile_count,
            COUNT(profile.internal_id) AS internal_profile_count
        FROM user LEFT JOIN profile
        ON user.id = profile.user_id
        GROUP BY user.id
        HAVING COUNT(profile.google_id) = 0
        ORDER BY user.id
    ''',

    # Categorize length of each booking by number of days.
    # For each user and each category, compute the average
    # price of bookings for that user and length category,
    # defined as the sum of prices divided by the sum of
    # lengths of each such booking.
    'avg_daily_price_by_user_by_booking_length_category': '''
        WITH category(id) AS ( -- just to be sure, in case a category is not represented in any results
            VALUES
                ('days'),
                ('weeks'),
                ('months'),
                ('years'),
                ('centuries')
        ),
        bookingext(id, user_id, price, duration, category_id) AS (
            SELECT id,
                   user_id,
                   price,
                   JULIANDAY(end_date) - JULIANDAY(start_date),
                   CASE
                       WHEN JULIANDAY(end_date) - JULIANDAY(start_date) >= 36525
                           THEN 'centuries'
                       WHEN JULIANDAY(end_date) - JULIANDAY(start_date) >=   365
                           THEN 'years'
                       WHEN JULIANDAY(end_date) - JULIANDAY(start_date) >=    30
                           THEN 'months'
                       WHEN JULIANDAY(end_date) - JULIANDAY(start_date) >=     7
                           THEN 'weeks'
                       WHEN JULIANDAY(end_date) - JULIANDAY(start_date) >=     0
                           THEN 'days'
                   END
            FROM booking
        )
        SELECT user.username,
               category.id AS duration,
               SUM(bookingext.price) / SUM(bookingext.duration) AS avg_daily_price
        FROM category
        CROSS JOIN user
        LEFT JOIN bookingext
        -- The Rust SQL parser is unable to parse the below tuple expression
        -- ON (user.id, category.id) = (bookingext.user_id, bookingext.category_id)
        ON user.id = bookingext.user_id AND category.id = bookingext.category_id
        GROUP BY user.username, category.id
        ORDER BY user.username, category.id
    ''',

    'top_n_booked_regions_for_user_x': {
        'sql':
        '''
        SELECT region_name, COUNT(region_name) AS frequency
        FROM (
            SELECT region.name AS region_name
            FROM booking
            INNER JOIN real_estate
                ON booking.real_estate_id = real_estate.id
            INNER JOIN region
                ON region.id = real_estate.region_id
            WHERE booking.user_id = ?
        )
        GROUP BY region_name
        ORDER BY frequency DESC, region_name
        LIMIT ?
        ''',
        'params': (333, 10),
    },

    'northest_booked_latitude_slow': '''
        SELECT user.id AS user_id, MAX(location.latitude) AS northest_latitude
        FROM user
        LEFT JOIN booking
            ON booking.user_id = user.id
        LEFT JOIN real_estate
            ON real_estate.id = booking.real_estate_id
        LEFT JOIN location
            ON location.region_id = real_estate.region_id
        GROUP BY user.id
        ORDER BY user.id
    ''',

    'northest_booked_latitude_fast': '''
        WITH max_latitude(region_id, latitude) AS (
            SELECT region.id, MAX(location.latitude)
            FROM region
            INNER JOIN location
            ON location.region_id = region.id
            GROUP BY region.id
        )
        SELECT user.id AS user_id,
               MAX(max_latitude.latitude) AS northest_latitude
        FROM user
        LEFT JOIN booking
            ON user.id = booking.user_id
        LEFT JOIN real_estate
            ON booking.real_estate_id = real_estate.id
        LEFT JOIN max_latitude
            ON max_latitude.region_id = real_estate.region_id
        GROUP BY user.id
        ORDER BY user.id
    ''',
}

def serialize_unknown(obj):
    if isinstance(obj, (bytes, bytearray)):
        return ''.join('{:02x}'.format(b) for b in obj)
    elif isinstance(obj, sqlite3.Row):
        return dict(obj)
    else:
        raise TypeError('type {} is not serializable as JSON'.format(type(obj)))

def compute_complexity_metrics(sql):
    result = subprocess.run(
        ['cargo', 'run', '--release', '--', 'complexity', '--dialect', 'sqlite'],
        cwd='complexity_metrics',
        capture_output=True,
        check=True,
        text=True,
        input=sql,
    )

    return json.loads(result.stdout)

def compute_token_histogram(sql):
    result = subprocess.run(
        ['cargo', 'run', '--release', '--', 'histogram', '--dialect', 'sqlite', '--input', '-'],
        cwd='complexity_metrics',
        capture_output=True,
        check=True,
        text=True,
        input=sql,
    )

    return json.loads(result.stdout)

def main(conn, queries):
    results = {}
    metrics = {}
    histogram = []

    for name, value in queries.items():
        if isinstance(value, str):
            sql = value
            params = ()
            ty = list
            row = lambda x: x
        elif isinstance(value, dict):
            sql = value['sql']
            params = value.get('params', ())
            ty = value.get('ty', list)
            row = value.get('row', lambda x: x)
        else:
            raise RuntimeError('unexpected query type: ' + str(type(value)))

        print('Executing Query:', name)
        before = time.time()

        cursor = conn.execute(sql, params)
        results[name] = {
            'params': params,
            'results': ty(row(r) for r in cursor),
        }

        after = time.time()
        elapsed = after - before

        metrics[name] = compute_complexity_metrics(sql)
        metrics[name]['elapsed'] = elapsed

        histogram.extend(
            DataFrame(
                {(name, row['token']): [row['frequency']]},
                index=['frequency']
            ).T
            for row in compute_token_histogram(sql)
        )

    metrics = DataFrame(metrics).T
    metrics.index.name = 'query'

    histogram = pd.concat(histogram)
    histogram.index.names = ['query', 'token']

    return results, metrics, histogram

if __name__ == '__main__':
    with sqlite3.connect('example_data_before_migration.sqlite3') as conn:
        conn.row_factory = sqlite3.Row # so that we get column names
        conn.execute('PRAGMA foreign_keys = 1')
        results, metrics, histogram = main(conn, queries)

    with open('query_results_before_migration.json', 'w') as file:
        json.dump(results,
                  file,
                  indent=2,
                  ensure_ascii=False,
                  allow_nan=False,
                  default=serialize_unknown)

    metrics.to_csv('metrics.csv')
    histogram.to_csv('histogram.csv')
