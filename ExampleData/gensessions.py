#!/usr/bin/env python3

import numpy as np
import json
import pickle
from datetime import datetime, timedelta
from mkdb import read_pickle

np.random.seed(13374242)

def parse_date(datestring):
    return datetime.strptime(datestring, dateformat)

def format_date(date):
    return datetime.strftime(date, dateformat)[:-3] if date else None

def hex_encode(bs):
    return ''.join('{:02x}'.format(b) for b in bs)

def bernoulli(p=0.5, size=None):
    if size is None:
        return bool(np.random.binomial(n=1, p=p))
    else:
        return np.random.binomial(n=1, p=p, size=size).astype(bool)

users = read_pickle('pickled_users.pkl')
pubkeys = { u['id']: [] for u in users }
num_sessions = np.array([3, 2, 0, 0, 2, 3, 2, 1, 1, 1, 4, 1, 1, 3, 2]) # generated from a Poisson distribution with lambda=1.75
num_profiles = np.array([1, 1, 3, 2, 1, 3, 1, 2, 1, 5, 2, 1, 2, 2, 2]) # generated from the same distribution until no 0s found
dateformat = '%Y-%m-%d %H:%M:%S.%f'
epoch = parse_date('2000-01-01 00:00:00.00')

profile_variants = ['facebook', 'google', 'internal']
image_extensions = ['jpg', 'jpeg', 'png', 'gif', 'tiff', 'bmp']
first_names = ['mate', 'mark', 'lukacs', 'janos', 'istvan', 'laszlo', 'miklos', 'maria', 'magdolna', 'anna']
last_names = ['kovacs', 'kiss', 'acs', 'suszter', 'nagy', 'gipsz', 'takacs', 'asztalos', 'naturwissenschaftler']
domains = ['gmail', 'yahoo', 'microsoft', 'rust-lang', 'adobe']
tlds = ['com', 'org', 'edu', 'biz', 'cc', 'info']

def generate_session(user_id):
    login_pubkey = np.random.bytes(32)
    login_date = epoch + timedelta(days=np.random.rand() * 20 * 365)

    pubkeys[user_id].append(login_pubkey)
    is_expired = bernoulli(p=0.5)

    if is_expired:
        logout_pubkey = np.random.choice(pubkeys[user_id])
        logout_date = login_date + timedelta(days=np.random.rand() * 2 * 365)

        pubkeys[user_id].remove(login_pubkey)
    else:
        logout_pubkey = None
        logout_date = None

    return {
        'login_public_key': login_pubkey,
        'login_date': format_date(login_date),
        'logout_public_key': logout_pubkey,
        'logout_date': format_date(logout_date),
    }

def generate_profile():
    variant = np.random.choice(profile_variants)

    if variant == 'facebook':
        profile = {
            'facebook_account_id': str(np.random.choice(2**63)),
        }
    elif variant == 'google':
        b1 = bernoulli(p=2/3)
        b2 = bernoulli(p=2/3)
        email = '{}.{}@{}.{}'.format(np.random.choice(first_names), np.random.choice(last_names), np.random.choice(domains), np.random.choice(tlds)) if b1 else None
        image_url_template = 'https://images.google.com/fictional-directory/path/{}.{}?size=big'
        image_url = image_url_template.format(hex_encode(np.random.bytes(8)), np.random.choice(image_extensions)) if b2 else None

        profile = {
            'google_account_id': hex_encode(np.random.bytes(16)),
            'email': email,
            'image_url': image_url,
        }
    elif variant == 'internal':
        profile = {
            'password_hash': np.random.bytes(64),
            'password_salt': np.random.bytes(16),
        }
    else:
        raise ValueError('invalid profile variant: ' + variant)

    return { 'variant': variant, **profile }

for u, n, k in zip(users, num_profiles, num_sessions):
    u['profiles'] = [generate_profile() for _ in range(n)]
    u['sessions'] = [generate_session(u['id']) for _ in range(k)]

with open('pickled_users.pkl', 'wb') as file:
   pickle.dump(users, file)

