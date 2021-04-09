#!/usr/bin/env python3

import sqlite3
import pickle

def read_pickle(filename):
    with open(filename, 'rb') as file:
        return pickle.load(file)

def create_schema(conn):
    conn.execute('''
    CREATE TABLE region(
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NULL,
        name TEXT NOT NULL UNIQUE,
        FOREIGN KEY (parent_id) REFERENCES region(id)
    )
    ''')
    conn.execute('CREATE INDEX region_parent_index ON region(parent_id)')

    conn.execute('''
    CREATE TABLE location(
        id INTEGER PRIMARY KEY,
        latitude REAL NOT NULL CHECK (latitude BETWEEN -90 AND 90),
        longitude REAL NOT NULL CHECK (longitude BETWEEN -180 AND 180),
        region_id INTEGER NOT NULL,
        FOREIGN KEY (region_id) REFERENCES region(id)
    )
    ''')
    conn.execute('CREATE INDEX location_region_index ON location(region_id)')

    conn.execute('''
    CREATE TABLE user(
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        real_name TEXT NULL,
        birth_date TEXT NULL
                   CHECK(birth_date IS STRFTIME('%Y-%m-%d %H:%M:%f', birth_date))
    )
    ''')
    # conn.execute('CREATE INDEX username_index ON user(username)') # UNIQUE, already indexed
    conn.execute('CREATE INDEX user_birth_date_index ON user(birth_date)')

    conn.execute('''
    CREATE TABLE session(
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        login_public_key BLOB NOT NULL UNIQUE
                         CHECK(LENGTH(login_public_key) == 32),
        login_date TEXT NOT NULL
                   CHECK(login_date IS STRFTIME('%Y-%m-%d %H:%M:%f', login_date)),
        logout_public_key BLOB NULL
                          CHECK(LENGTH(logout_public_key) == 32),
        logout_date TEXT NULL
                    CHECK(logout_date IS STRFTIME('%Y-%m-%d %H:%M:%f', logout_date)),
        FOREIGN KEY (user_id) REFERENCES user(id),
        CHECK (
            (logout_public_key IS NULL AND logout_date IS NULL)
            OR
            (logout_public_key IS NOT NULL AND logout_date IS NOT NULL)
        )
    )
    ''')
    conn.execute('CREATE INDEX session_user_index ON session(user_id)')
    conn.execute('CREATE INDEX session_logout_public_key_index ON session(logout_public_key)')

    conn.execute('''
    CREATE TABLE profile_facebook(
        id INTEGER PRIMARY KEY,
        facebook_account_id TEXT NOT NULL UNIQUE
    )
    ''')

    conn.execute('''
    CREATE TABLE profile_google(
        id INTEGER PRIMARY KEY,
        google_account_id TEXT NOT NULL UNIQUE,
        email TEXT NULL UNIQUE,
        image_url TEXT NULL
    )
    ''')

    conn.execute('''
    CREATE TABLE profile_internal(
        id INTEGER PRIMARY KEY,
        password_hash BLOB NOT NULL
                      CHECK(LENGTH(password_hash) = 64),
        password_salt BLOB NOT NULL UNIQUE
                      CHECK(LENGTH(password_salt) = 16)
    )
    ''')

    conn.execute('''
    CREATE TABLE profile(
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        facebook_id INTEGER NULL UNIQUE,
        google_id INTEGER NULL UNIQUE,
        internal_id INTEGER NULL UNIQUE,
        FOREIGN KEY (user_id) REFERENCES user(id),
        FOREIGN KEY (facebook_id) REFERENCES profile_facebook(id),
        FOREIGN KEY (google_id) REFERENCES profile_google(id),
        FOREIGN KEY (internal_id) REFERENCES profile_internal(id),
        CHECK(
            (facebook_id IS NOT NULL) + (google_id IS NOT NULL) + (internal_id IS NOT NULL) = 1
        )
    )
    ''')
    conn.execute('CREATE INDEX profile_user_index ON profile(user_id)')
    conn.execute('CREATE INDEX profile_facebook_index ON profile(facebook_id)')
    conn.execute('CREATE INDEX profile_google_index ON profile(google_id)')
    conn.execute('CREATE INDEX profile_internal_index ON profile(internal_id)')

    conn.execute('''
    CREATE TABLE real_estate(
        id INTEGER PRIMARY KEY,
        kind TEXT NOT NULL
             CHECK (kind IN ('apartment', 'house', 'mansion', 'penthouse')),
        owner_id INTEGER NOT NULL,
        region_id INTEGER NOT NULL,
        FOREIGN KEY (owner_id) REFERENCES user(id),
        FOREIGN KEY (region_id) REFERENCES region(id)
    )
    ''')
    conn.execute('CREATE INDEX real_estate_owner_index ON real_estate(owner_id)')
    conn.execute('CREATE INDEX real_estate_region_index ON real_estate(region_id)')

    conn.execute('''
    CREATE TABLE booking(
        id INTEGER PRIMARY KEY,
        real_estate_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        start_date TEXT NOT NULL
                   CHECK(start_date IS STRFTIME('%Y-%m-%d %H:%M:%f', start_date)),
        end_date TEXT NOT NULL
                 CHECK(end_date IS STRFTIME('%Y-%m-%d %H:%M:%f', end_date)),
        price DECIMAL(12, 2) NOT NULL
              CHECK(price >= 0.0),
        FOREIGN KEY (real_estate_id) REFERENCES real_estate(id),
        FOREIGN KEY (user_id) REFERENCES user(id)
    )
    ''')
    conn.execute('CREATE INDEX booking_real_estate_index ON booking(real_estate_id)')
    conn.execute('CREATE INDEX booking_user_index ON booking(user_id)')

def insert_regions(conn, regions):
    for r in regions:
        conn.execute(
            '''
            INSERT INTO region(id, parent_id, name)
            VALUES (?, ?, ?)
            ''',
            (r['id'], r['parent_id'], r['name'])
        )

        for b in r['bounds']:
            conn.execute(
                '''
                INSERT INTO location(latitude, longitude, region_id)
                VALUES (?, ?, ?)
                ''',
                (b['latitude'], b['longitude'], r['id'])
            )

def insert_users(conn, users):
    for u in users:
        conn.execute(
            '''
            INSERT INTO user(id, username, real_name, birth_date)
            VALUES (?, ?, ?, ?)
            ''',
            (u['id'], u['username'], u['real_name'], u['birth_date'])
        )

        for p in u['profiles']:
            variant = p['variant']
            items = [(k, v) for k, v in p.items() if k != 'variant']

            cursor = conn.execute(
                'INSERT INTO profile_{}({}) VALUES({})'.format(
                    variant,
                    ', '.join(k for k, v in items),
                    ', '.join(['?'] * len(items))
                ),
                tuple(v for k, v in items)
            )
            rowid = cursor.lastrowid

            conn.execute(
                'INSERT INTO profile (user_id, {}_id) VALUES (?, ?)'.format(variant),
                (u['id'], rowid,)
            )

        for s in u['sessions']:
            conn.execute(
                '''
                INSERT INTO session(user_id, login_public_key, login_date, logout_public_key, logout_date)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (u['id'], s['login_public_key'], s['login_date'], s['logout_public_key'], s['logout_date'])
            )

def insert_real_estates(conn, real_estates):
    for r in real_estates:
        conn.execute(
            '''
            INSERT INTO real_estate(id, kind, owner_id, region_id)
            VALUES (?, ?, ?, ?)
            ''',
            (r['id'], r['kind'], r['owner_id'], r['region_id'])
        )

def insert_bookings(conn, bookings):
    for b in bookings:
        conn.execute(
            '''
            INSERT INTO booking(id, real_estate_id, user_id, start_date, end_date, price)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (b['id'], b['real_estate_id'], b['user_id'], b['start_date'], b['end_date'], b['price'])
        )

if __name__ == '__main__':
    regions = read_pickle('pickled_regions.pkl')
    users = read_pickle('pickled_users.pkl')
    real_estates = read_pickle('pickled_real_estates.pkl')
    bookings = read_pickle('pickled_bookings.pkl')

    with sqlite3.connect('example_data_before_migration.sqlite3') as conn:
        conn.execute('PRAGMA foreign_keys = 1')
        create_schema(conn)
        insert_regions(conn, regions)
        insert_users(conn, users)
        insert_real_estates(conn, real_estates)
        insert_bookings(conn, bookings)
