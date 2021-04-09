from datetime import datetime, timezone

# Helper for renaming `id` columns to the conventional
# `_id` field because that is auto-indexed by MongoDB.
def rename_id_dict(row):
    d = dict(row)
    d['_id'] = d['id']
    del d['id']
    return d

def date_from_string(s):
    if s is None:
        return None
    else:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)

def transform_date_fields(rows, *fields):
    for row in rows:
        for field in fields:
            row[field] = date_from_string(row[field])

# The tree structure of regions would allow us to transform
# the tree of regions into a single, giant nested document
# with `children` arrays at each level. However, this is not
# possible for general graphs without duplicating each node
# every time it appears in the graph.
#
# It is also not easy or efficient to search a single big
# document with arbitrary structure (think of indexing!),
# and AFAICT it's not even possible to do that recursively
# using MongoDB.
# It _is_, however, possible to perform a proper recursive
# query using the `$graphLookup` aggregation stage, if the
# graph nodes are themselves separate documents. So here we
# opt for keeping the implicit hierarchy using parent IDs.
def load_regions(conn):
    rows = conn.execute('''
        WITH tmp AS (
                SELECT id, name, parent_id, CAST(id AS TEXT) AS path
                FROM region
                WHERE parent_id IS NULL
            UNION ALL
                SELECT region.id, region.name, region.parent_id,
                       (tmp.path || '/' || CAST(region.id AS TEXT)) AS path
                FROM region
                INNER JOIN tmp
                ON region.parent_id = tmp.id
        )
        SELECT id as _id, name, parent_id
        FROM tmp
        ORDER BY path
    ''')
    regions = {row['_id']: {**row, 'bounds': []} for row in rows}

    rows = conn.execute(
        'SELECT latitude, longitude, region_id FROM location ORDER BY region_id, id'
    )

    for row in rows:
        regions[row['region_id']]['bounds'].append({
            'latitude': row['latitude'],
            'longitude': row['longitude'],
        })

    return list(regions.values())

def load_profiles(conn):
    rows = conn.execute('''
        SELECT profile.*,
               profile_facebook.facebook_account_id,
               profile_google.google_account_id,
               profile_google.image_url,
               profile_google.email,
               profile_internal.password_hash,
               profile_internal.password_salt
        FROM profile
        LEFT JOIN profile_facebook
        ON profile_facebook.id = profile.facebook_id
        LEFT JOIN profile_google
        ON profile_google.id = profile.google_id
        LEFT JOIN profile_internal
        ON profile_internal.id = profile.internal_id
    ''')

    profile_by_user = {}

    for row in rows:
        profiles = profile_by_user.setdefault(row['user_id'], [])

        if row['facebook_id'] is not None:
            profiles.append({
                'type': 'facebook',
                'facebook_account_id': row['facebook_account_id'],
            })
        elif row['google_id'] is not None:
            profiles.append({
                'type': 'google',
                'google_account_id': row['google_account_id'],
                'email': row['email'],
                'image_url': row['image_url'],
            })
        elif row['internal_id'] is not None:
            profiles.append({
                'type': 'internal',
                'password_hash': row['password_hash'],
                'password_salt': row['password_salt'],
            })

    return profile_by_user

def auth_event(row, prefix):
    pubkey_key = prefix + '_public_key'
    date_key = prefix + '_date'

    if row[pubkey_key] is None:
        return None
    else:
        return {
            'public_key': row[pubkey_key],
            'date': date_from_string(row[date_key]),
        }

def load_sessions(conn):
    rows = conn.execute('SELECT * FROM session')
    session_by_user = {}

    for row in rows:
        session_by_user.setdefault(row['user_id'], []).append({
            'login': auth_event(row, 'login'),
            'logout': auth_event(row, 'logout'),
        })

    return session_by_user

def load_users(conn):
    users = [rename_id_dict(row) for row in conn.execute('SELECT * FROM user')]

    profiles = load_profiles(conn)
    sessions = load_sessions(conn)

    for user in users:
        user_id = user['_id']
        user['profiles'] = profiles.get(user_id, [])
        user['sessions'] = sessions.get(user_id, [])

    transform_date_fields(users, 'birth_date')

    return users

def load_real_estates(conn):
    return [rename_id_dict(row) for row in conn.execute('SELECT * FROM real_estate')]

def load_bookings(conn):
    bookings = [rename_id_dict(row) for row in conn.execute('SELECT * FROM booking')]
    transform_date_fields(bookings, 'start_date', 'end_date')
    return bookings

def create_indexes(db):
    db.region.create_index('parent_id')

    db.user.create_index('username', unique=True)
    db.user.create_index('birth_date')
    db.user.create_index('sessions.logout')
    db.user.create_index('profiles.type')

    db.real_estate.create_index('owner_id')
    db.real_estate.create_index('region_id')

    db.booking.create_index('user_id')
    db.booking.create_index('real_estate_id')

def populate_database(input_conn, output_db):
    '''
    `input_conn` should be a SQLite3 connection with
    the relational example data.
    `output_conn` should be a PyMongo database
    where the MongoDB schema will be created.
    '''
    regions = load_regions(input_conn)
    users = load_users(input_conn)
    real_estates = load_real_estates(input_conn)
    bookings = load_bookings(input_conn)

    output_db.region.insert_many(regions)
    output_db.user.insert_many(users)
    output_db.real_estate.insert_many(real_estates)
    output_db.booking.insert_many(bookings)

    create_indexes(output_db)
