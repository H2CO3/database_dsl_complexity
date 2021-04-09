import json
import time
import metrics

from collections import namedtuple
from pandas import DataFrame
from pymongo import ASCENDING, DESCENDING


ResultSet = namedtuple(
    'ResultSet',
    ['name', 'actual', 'expected', 'violation_indexes', 'elapsed_time'],
)
Metrics = namedtuple(
    'Metrics',
    [
        'query',
        'token_count', 'token_entropy',
        'node_count', 'weighted_node_count',
        'halstead_vocabulary', 'halstead_length', 'halstead_estimated_length',
        'halstead_volume', 'halstead_difficulty', 'halstead_effort',
    ],
)

class Queries:
    def __init__(self, db, test_cases):
        self.db = db
        self.test_cases = test_cases

    def execute(self):
        prefix = 'query_'
        queries = {
            name.lstrip(prefix): getattr(self, name)
            for name in dir(self)
            if name.startswith(prefix)
        }

        retvals = [self._results_for_query(name, fn) for name, fn in queries.items()]

        # Transpose
        results = [rv[0] for rv in retvals]
        metrics = [rv[1] for rv in retvals]

        return results, DataFrame(metrics).set_index('query')

    def _results_for_query(self, name, fn):
        tc = self.test_cases[name]
        collection, pipeline = fn(tc.params)

        t0 = time.time()
        actual = list(collection.aggregate(pipeline))
        tf = time.time()
        elapsed = tf - t0

        violations = tc.violation_indexes(actual)
        result = ResultSet(name, actual, tc.expected, violations, elapsed)

        query_string = json.dumps(pipeline)
        tokens = list(metrics.bson_tokenize(query_string))
        metric = Metrics(
            query=name,
            token_count=metrics.token_count(tokens),
            token_entropy=metrics.token_entropy(tokens),
            node_count=metrics.node_count(pipeline),
            weighted_node_count=metrics.weighted_node_count(pipeline),
            **metrics.halstead(tokens),
        )

        return result, metric

    # Queries start below.
    # By convention, query methods take a `params` argument,
    # and their name starts with `query_` (how creative).
    # This helps us retrieve and them using reflection.
    # They return a (collection, pipeline) tuple, so that
    # `collection.aggregate(pipeline)` yields the results.

    def query_continents(self, params):
        return self.db.region, [
            {
                '$match': { 'parent_id': None }
            },
            {
                '$project': {
                    'id': '$_id',
                    '_id': False,
                    'parent_id': True,
                    'name': True,
                }
            },
            {
                '$sort': { 'id': ASCENDING },
            },
        ]

    def query_siblings_and_parents(self, params):
        return self.db.region, [
            {
                '$match': { '_id': params[0] }
            },
            {
                '$graphLookup': {
                    'from': 'region',
                    'startWith': '$_id',
                    'connectFromField': 'parent_id',
                    'connectToField': '_id',
                    'as': 'lineage',
                }
            },
            {
                '$project': {
                    'lineage': {
                        '$map': {
                            'input': '$lineage',
                            'as': 'item',
                            'in': {
                                '_id': '$$item._id',
                                'parent_id': '$$item.parent_id',
                                'name': '$$item.name',
                            }
                        }
                    }
                }
            },
            {
                '$unwind': '$lineage'
            },
            {
                '$lookup': {
                    'from': 'region',
                    'localField': 'lineage._id',
                    'foreignField': 'parent_id',
                    'as': 'children',
                }
            },
            {
                '$unwind': '$children'
            },
            {
                '$project': {
                    '_id': False,
                    'id': '$children._id',
                    'parent_id': '$children.parent_id',
                    'name': '$children.name',
                }
            },
            {
                '$unionWith': {
                    'coll': 'region',
                    'pipeline': [
                        {
                            '$match': { 'parent_id': None }
                        },
                        {
                            '$project': {
                                '_id': False,
                                'id': '$_id',
                                'parent_id': '$parent_id',
                                'name': '$name',
                            }
                        },
                    ],
                }
            },
            {
                '$sort': {
                    'parent_id': DESCENDING,
                    'id': ASCENDING,
                }
            },
        ]

    def query_no_login_users(self, params):
        return self.db.user, [
            {
                '$match': { 'sessions': [] }
            },
            {
                '$project': {
                    'user_id': '$_id',
                    '_id': False,
                },
            },
            {
                '$sort': { 'user_id': ASCENDING }
            },
        ]

    def query_num_valid_sessions(self, params):
        return self.db.user, [
            {
                '$project': {
                    '_id': False,
                    'user_id': '$_id',
                    'valid_sessions': {
                        '$size': {
                            '$filter': {
                                'input': '$sessions',
                                'as': 'item',
                                'cond': {
                                    '$eq': ['$$item.logout', None]
                                }
                            }
                        }
                    }
                }
            },
            {
                '$sort': {
                    'user_id': ASCENDING,
                }
            },
        ]

    def query_multi_profile_users(self, params):
        return self.db.user, [
            {
                '$project': {
                    '_id': False,
                    'user_id': '$_id',
                    'profile_count': {
                        '$size': '$profiles'
                    }
                }
            },
            {
                '$match': {
                    'profile_count': { '$gt': 1 }
                }
            },
            {
                '$sort': {
                    'user_id': ASCENDING,
                }
            },
        ]

    def query_owned_real_estate_region_count(self, params):
        return self.db.user, [
            {
                '$lookup': {
                    'from': 'real_estate',
                    'localField': '_id',
                    'foreignField': 'owner_id',
                    'as': 'owned_real_estates',
                }
            },
            {
                '$project': {
                    '_id': False,
                    'user_id': '$_id',
                    'region_count': {
                        '$size': {
                            # `setDifference` between an array and an empty array is
                            # a trick for removing duplicates from the first array,
                            # as the operator implicitly treats both arrays as sets.
                            '$setDifference': ['$owned_real_estates.region_id', []]
                        }
                    },
                }
            },
            {
                '$sort': {
                    'user_id': ASCENDING,
                }
            },
        ]

    def query_profile_counts_by_non_google_user(self, params):
        return self.db.user, [
            {
                '$project': {
                    '_id': False,
                    'user_id': '$_id',
                    'fb_profile_count': {
                        '$size': {
                            '$filter': {
                                'input': '$profiles',
                                'as': 'item',
                                'cond': {
                                    '$eq': ['$$item.type', 'facebook']
                                }
                            }
                        }
                    },
                    'google_profile_count': {
                        '$size': {
                            '$filter': {
                                'input': '$profiles',
                                'as': 'item',
                                'cond': {
                                    '$eq': ['$$item.type', 'google']
                                }
                            }
                        }
                    },
                    'internal_profile_count': {
                        '$size': {
                            '$filter': {
                                'input': '$profiles',
                                'as': 'item',
                                'cond': {
                                    '$eq': ['$$item.type', 'internal']
                                }
                            }
                        }
                    },
                }
            },
            {
                '$match': {
                    'google_profile_count': 0,
                }
            },
            {
                '$unset': 'google_profile_count'
            },
            {
                '$sort': {
                    'user_id': ASCENDING,
                }
            },
        ]

    def query_avg_daily_price_by_user_by_booking_length_category(self, params):
        return self.db.booking, [
            {
                '$addFields': {
                    'days': {
                        '$divide': [
                            {
                                '$subtract': ['$end_date', '$start_date']
                            },
                            # Subtracting dates results in milliseconds.
                            # This is not exactly correct because in SQL we work
                            # with the Julian day count, but MongoDB doesn't
                            # support that, and this approximation/error is
                            # accounted for by the rounding in the approximate
                            # comparison of floating-point numbers in `TestCase`.
                            24 * 3600 * 1000
                        ]
                    },
                },
            },
            # This stage just computes the category based on the duration of each booking.
            {
                '$addFields': {
                    'duration': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': { '$gte': ['$days', 36525] },
                                    'then': 'centuries',
                                },
                                {
                                    'case': { '$gte': ['$days',   365] },
                                    'then': 'years',
                                },
                                {
                                    'case': { '$gte': ['$days',    30] },
                                    'then': 'months',
                                },
                                {
                                    'case': { '$gte': ['$days',     7] },
                                    'then': 'weeks',
                                },
                                {
                                    'case': { '$gte': ['$days',     0] },
                                    'then': 'days',
                                },
                            ],
                        }
                    },
                }
            },
            # Compute average daily price by user by duration category
            {
                '$group': {
                    '_id': ['$user_id', '$duration'],
                    'total_price': { '$sum': '$price' },
                    'total_days': { '$sum': '$days' },
                }
            },
            {
                '$project': {
                    '_id': False,
                    'user_id': { '$first': '$_id' },
                    'username': None,
                    'duration': { '$last': '$_id' },
                    'avg_daily_price': {
                        '$divide': ['$total_price', '$total_days']
                    },
                }
            },
            # Add (user, category) combinations not found in results so far.
            # Augment with usernames too, since we'll need them in a moment.
            {
                '$unionWith': {
                    'coll': 'user',
                    'pipeline': [
                        {
                            '$addFields': {
                                'category': [
                                    'days',
                                    'weeks',
                                    'months',
                                    'years',
                                    'centuries',
                                ]
                            }
                        },
                        {
                            '$unwind': '$category',
                        },
                        {
                            '$project': {
                                '_id': False,
                                'user_id': '$_id',
                                'username': True,
                                'duration': '$category',
                                'avg_daily_price': None,
                            }
                        },
                    ],
                },
            },
            {
                '$group': {
                    '_id': ['$user_id', '$duration'],
                    'username': { '$max': '$username' },
                    'avg_daily_price': { '$max': '$avg_daily_price' },
                }
            },
            # Prepare results
            {
                '$project': {
                    '_id': False,
                    'username': True,
                    'duration': { '$last': '$_id' },
                    'avg_daily_price': True,
                }
            },
            {
                '$sort': {
                    'username': ASCENDING,
                    'duration': ASCENDING,
                }
            },
        ]

    def query_top_n_booked_regions_for_user_x(self, params):
        user_id, top_n = params

        return self.db.booking, [
            {
                '$match': { 'user_id': user_id }
            },
            {
                '$lookup': {
                    'from': 'real_estate',
                    'localField': 'real_estate_id',
                    'foreignField': '_id',
                    'as': 'real_estate',
                }
            },
            {
                '$unwind': '$real_estate'
            },
            {
                '$project': {
                    'region_id': '$real_estate.region_id'
                }
            },
            {
                '$group': {
                    '_id': '$region_id',
                    'frequency': { '$sum': 1 }, # count
                }
            },
            {
                '$lookup': {
                    'from': 'region',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'region',
                }
            },
            {
                '$unwind': '$region'
            },
            {
                '$project': {
                    '_id': False,
                    'frequency': True,
                    'region_name': '$region.name'
                }
            },
            {
                '$sort': {
                    'frequency': DESCENDING,
                    'region_name': ASCENDING,
                }
            },
            {
                '$limit': top_n
            },
        ]

    def query_northest_booked_latitude_slow(self, params):
        return self.db.booking, [
            # Join real estates to bookings and project region ID
            {
                '$lookup': {
                    'from': 'real_estate',
                    'localField': 'real_estate_id',
                    'foreignField': '_id',
                    'as': 'real_estate',
                }
            },
            {
                '$unwind': {
                    'path': '$real_estate',
                    'preserveNullAndEmptyArrays': True,
                }
            },
            {
                '$project': {
                    '_id': False,
                    'user_id': True,
                    'region_id': '$real_estate.region_id',
                }
            },
            # Filter for unique (users, region) combinations.
            # This is an optimization, so that the next `$lookup`
            # has less redundant work to do.
            {
                '$group': {
                    '_id': ['$user_id', '$region_id'],
                }
            },
            {
                '$project': {
                    '_id': False,
                    'user_id': { '$first': '$_id' },
                    'region_id': { '$last': '$_id' },
                }
            },
            # Join regions
            {
                '$lookup': {
                    'from': 'region',
                    'localField': 'region_id',
                    'foreignField': '_id',
                    'as': 'region',
                }
            },
            {
                '$unwind': '$region'
            },
            # Compute maximum latitude over all regions for each user
            {
                '$unwind': '$region.bounds'
            },
            {
                '$group': {
                    '_id': '$user_id',
                    'northest_latitude': {
                        '$max': '$region.bounds.latitude'
                    },
                }
            },
            # Add users with no bookings
            {
                '$unionWith': {
                    'coll': 'user',
                    'pipeline': [
                        {
                            '$project': {
                                'northest_latitude': None,
                            }
                        },
                    ]
                }
            },
            {
                '$group': {
                    '_id': '$_id',
                    'northest_latitude': {
                        '$max': '$northest_latitude'
                    },
                }
            },
            # Prepare and format output
            {
                '$project': {
                    '_id': False,
                    'user_id': '$_id',
                    'northest_latitude': True,
                }
            },
            {
                '$sort': {
                    'user_id': ASCENDING,
                }
            },
        ]

    def query_northest_booked_latitude_fast(self, params):
        return self.db.region, [
            # Precompute northest latitude for each region
            {
                '$project': {
                    '_id': False,
                    'region_id': '$_id',
                    'northest_latitude': {
                        '$max': '$bounds.latitude'
                    },
                }
            },
            # Use that for precomputing northest latitude for each real estate
            {
                '$lookup': {
                    'from': 'real_estate',
                    'localField': 'region_id',
                    'foreignField': 'region_id',
                    'as': 'real_estate',
                }
            },
            {
                '$unwind': '$real_estate',
            },
            {
                '$project': {
                    '_id': False,
                    'northest_latitude': True,
                    'real_estate_id': '$real_estate._id',
                }
            },
            # Join with users through bookings
            {
                '$lookup': {
                    'from': 'booking',
                    'localField': 'real_estate_id',
                    'foreignField': 'real_estate_id',
                    'as': 'booking',
                }
            },
            {
                '$unwind': '$booking',
            },
            {
                '$project': {
                    '_id': '$booking.user_id',
                    'northest_latitude': True,
                }
            },
            {
                '$group': {
                    '_id': '$_id',
                    'northest_latitude': {
                        '$max': '$northest_latitude'
                    },
                }
            },
            # Union with users without bookings
            {
                '$unionWith': {
                    'coll': 'user',
                    'pipeline': [
                        {
                            '$project': {
                                'northest_latitude': None,
                            }
                        },
                    ],
                }
            },
            {
                '$group': {
                    '_id': '$_id',
                    'northest_latitude': {
                        '$max': '$northest_latitude'
                    },
                }
            },
            # Prepare and format output
            {
                '$project': {
                    '_id': False,
                    'user_id': '$_id',
                    'northest_latitude': True,
                }
            },
            {
                '$sort': {
                    'user_id': ASCENDING,
                }
            },
        ]
