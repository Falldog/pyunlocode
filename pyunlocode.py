#!/usr/bin/env python
# 
# Create on : 2015/04/19
#
# @author : Falldog
#
import os
import csv
import sqlite3
from os.path import join


CURDIR = os.path.abspath(os.path.dirname(__file__))
CSVDIR = join(CURDIR, 'csv')
DB_PATH = join(CURDIR, 'unlocode.db')


class City():
    def __init__(self, row):
        self.code = row[0]
        self.name = row[1]
        self.coordinate = row[2]


class Country():
    def __init__(self, row):
        self.code = row[0]
        self.name = row[1]


class UnLocode():
    """
    Download from : http://www.unece.org/cefact/codesfortrade/codes_index.html
    Column Spec : http://www.unece.org/fileadmin/DAM/cefact/locode/Service/LocodeColumn.htm

    Just store City & Country for "code", "name" & "coordinate"
    """
    def __init__(self):
        self.conn = None

    def init(self):
        self.conn = sqlite3.connect(DB_PATH)

        c = self.conn.cursor()

        c.executescript('''
            CREATE TABLE IF NOT EXISTS country (
                code text PRIMARY KEY,
                name text
            );
            CREATE TABLE IF NOT EXISTS city (
                code text PRIMARY KEY,
                name text,
                coordinate text
            );
            CREATE UNIQUE INDEX IF NOT EXISTS city_code_index ON city(code);
        ''')
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_country_name(self, code):
        """ return None if could not found """
        c = self.conn.cursor()
        c.execute('SELECT name FROM country WHERE code = ?', (code,))
        r = c.fetchone()
        return r[0] if r else None

    def get_city_name(self, code):
        """ return None if could not found """
        c = self.conn.cursor()
        c.execute('SELECT name FROM city WHERE code = ?', (code,))
        r = c.fetchone()
        return r[0] if r else None

    def search_country_name_like(self, name):
        """ return [] if could not found """
        c = self.conn.cursor()
        c.execute('SELECT * FROM country WHERE name LIKE "%%%s%%"' % name)
        return [Country(country) for country in c.fetchall()]

    def search_city_name_like(self, name):
        """ return [] if could not found """
        c = self.conn.cursor()
        c.execute('SELECT * FROM city WHERE name LIKE "%%%s%%"' % name)
        return [City(city) for city in c.fetchall()]

    def gen_from_csv(self):
        c = self.conn.cursor()
        city_set = set()
        for filename in os.listdir(CSVDIR):
            if os.path.splitext(filename)[1] != '.csv':
                continue

            with open(join(CSVDIR, filename), 'rb') as f:
                data_reader = csv.reader(f, delimiter=',', quotechar='"')
                for row in data_reader:
                    country_code = row[1]

                    if row[3] and row[3][0] == '.':  # country name
                        name = row[3].decode('latin-1')[1:]  # ISO-8859-1, filter the first char "."
                        name = name.split(',')[0]
                        c.execute("INSERT OR REPLACE INTO country VALUES (?,?)", (country_code, name))

                    else:  # city name
                        city_code = country_code + row[2]
                        if len(city_code) <= 2:
                            print '*** skip invalid city code : ', city_code
                            continue

                        if city_code in city_set:
                            print '*** skip duplicate city code : ', city_code
                            continue
                        else:
                            city_set.add(city_code)

                        name = row[4]
                        coordinate = row[10] or ''
                        c.execute("INSERT OR REPLACE INTO city VALUES (?,?,?)", (city_code, name, coordinate))

        self.conn.commit()


def main():
    u = UnLocode()
    u.init()
    u.gen_from_csv()
    print u.get_country_name('US')
    print u.get_city_name('TWTPE')
    r = u.search_city_name_like('LOS ANGELES')
    for c in r:
        print "code:%s name:%s" % (c.code, c.name)
    u.close()

if __name__ == '__main__':
    main()

