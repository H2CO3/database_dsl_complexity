#!/usr/bin/env python3

import re
import sqlite3

from typing import Optional, NamedTuple
from datetime import datetime, timezone
from argparse import ArgumentParser
from urllib import request
from bs4 import BeautifulSoup


HOSTPROTO = 'https://data.stackexchange.com'
URL_RX = re.compile(r'stackoverflow/query/(?P<id>\d+)')

class Snippet(NamedTuple):
    id: int
    url: str
    title: str
    description: Optional[str]
    view_count: int
    fav_count: int
    author_name: Optional[str]
    author_id: Optional[int]
    created_at: datetime

    @staticmethod
    def new(element):
        link = element.find(name='div', attrs={'class': 'title'}).a
        views = element.find(name='div', attrs={'class': 'views'})
        favs = element.find(name='div', attrs={'class': 'favorites'}).b
        user = element.find(name='div', attrs={'class': 'user'})

        return Snippet(
            id = int(URL_RX.search(link['href']).group('id')),
            url=HOSTPROTO + link['href'].strip(),
            title=link.text.strip(),
            description=link.get('title', '').strip(),
            view_count=int(
                views.find(name='span', attrs={'class': 'pretty-short'})['title'] \
                    .strip() \
                    .replace(',', '')),
            fav_count=int(favs.text.strip()),
            author_name=user.a.text.strip() if user.a else None,
            author_id=int(user.a['href'].lstrip('/users/').strip()) if user.a else None,
            created_at=datetime.strptime(
                user.span['title'].strip().replace('Z', '+0000'),
                '%Y-%m-%d %H:%M:%S%z',
            )
        )

    def download_code(self):
        doc = doc_at_url(self.url)
        elem = doc.find(name='pre', attrs={'id': 'queryBodyText'})
        return elem.code.text.strip()

def doc_at_url(url):
    with request.urlopen(url) as fp:
        contents = fp.read().decode('utf8')

    return BeautifulSoup(contents, features='html.parser')

def snippets_on_page(n, size=100):
    assert isinstance(n, int)
    assert n > 0

    assert isinstance(size, int)
    assert size > 0

    url = '{}/stackoverflow/queries?order_by=favorite&page={}&pagesize={}'
    doc = doc_at_url(url.format(HOSTPROTO, n, size))
    elements = doc \
        .find(name='ul', attrs={'class': 'querylist'}) \
        .find_all(name='li')
    
    return [Snippet.new(elem) for elem in elements]

def now():
    return datetime.now(tz=None).strftime('%Y-%m-%d %H:%M:%S%z')

def ensure_schema(db):
    db.execute('''
    CREATE TABLE IF NOT EXISTS snippet(
        id INTEGER PRIMARY KEY CHECK(id > 0),
        url TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NULL,
        view_count INTEGER NOT NULL CHECK(view_count >= 0),
        fav_count INTEGER NOT NULL CHECK(fav_count >= 0),
        author_name TEXT NULL,
        author_id INTEGER NULL CHECK(author_id > 0),
        created_at TEXT NOT NULL
                   CHECK(created_at IS STRFTIME('%Y-%m-%d %H:%M:%S', created_at)),
        code TEXT NOT NULL
    )
    ''')
    db.execute('CREATE INDEX IF NOT EXISTS view_count_index ON snippet(view_count)')
    db.execute('CREATE INDEX IF NOT EXISTS fav_count_index ON snippet(fav_count)')
    db.execute('CREATE INDEX IF NOT EXISTS author_name_index ON snippet(author_name)')
    db.execute('CREATE INDEX IF NOT EXISTS author_id_index ON snippet(author_id)')
    db.execute('CREATE INDEX IF NOT EXISTS created_at_index ON snippet(created_at)')

def insert_snippet(db, snippet, code):
    args = snippet._asdict()
    
    args['created_at'] = snippet.created_at \
            .astimezone(timezone.utc) \
            .strftime('%Y-%m-%d %H:%M:%S')
    
    args['code'] = code

    db.execute('''
    INSERT INTO snippet(
        id,
        url,
        title,
        description,
        view_count,
        fav_count,
        author_name,
        author_id,
        created_at,
        code
    )
    VALUES(
        :id,
        :url,
        :title,
        :description,
        :view_count,
        :fav_count,
        :author_name,
        :author_id,
        :created_at,
        :code
    )
    ''', args)

def parse_args():
    parser = ArgumentParser(
        description='Download most favorited SQL snippets from Stack Exchange Data Explorer'
    )
    
    parser.add_argument('--outfile', type=str, required=False,
                        default='sede_stackoverflow_favorites.sqlite3',
                        help='SQLite3 database to write output to')

    # At the time of writing this function, there are 74 pages, 100 links each.
    parser.add_argument('--firstpage', type=int, required=False,
                        default=1, help='First page to retrieve')
    parser.add_argument('--lastpage', type=int, required=False,
                        default=74, help='Last page to retrieve')
    parser.add_argument('--pagesize', type=int, required=False,
                        default=100, help='Page size')

    return parser.parse_args()

def main():
    args = parse_args()
    db = sqlite3.connect(args.outfile)
    ensure_schema(db)

    for page in range(args.firstpage, args.lastpage + 1):
        print('[{}] Retrieving links on page #{}'.format(now(), page))

        snippets = snippets_on_page(n=page, size=args.pagesize)

        # Insert every page in its own transaction for incremental
        # updating, in case something goes wrong in the meantime.
        db.execute('BEGIN EXCLUSIVE TRANSACTION')

        for snippet in snippets:
            print('[{}]    Retrieving snippet ID #{}/{}'.format(now(), page, snippet.id))
            
            code = snippet.download_code()
            insert_snippet(db, snippet, code)
        
        db.execute('COMMIT')

    # Defragment the database because ROWIDs weren't inserted in order
    db.execute('VACUUM')

if __name__ == '__main__':
    main()
