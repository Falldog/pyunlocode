#!/usr/bin/env python3
"""Module to interface with UN/LOCODE """
#
# Create on : 2015/04/19
#
# @author : Falldog
#
import os
import math
import sqlite3

from os.path import join

from . import parser


CURDIR = os.path.abspath(os.path.dirname(__file__))
CSVDIR = join(CURDIR, 'csv')
DB_PATH = join(CURDIR, 'unlocode.db')


class PyUnLocode:
    """
    Download from : http://www.unece.org/cefact/codesfortrade/codes_index.html
    Column Spec : http://www.unece.org/fileadmin/DAM/cefact/locode/Service/LocodeColumn.htm
    """

    common_country_errors = {
        'COTE D\'IVOIRE': 'C\xd4TE D\'IVOIRE',
        'ENGLAND': 'UNITED KINGDOM',
        'RUSSIA': 'RUSSIAN FEDERATION',
        'REUNION': 'R\xc9UNION',
        'PEOPLE\'S REPUBLIC OF CHINA': 'CHINA',
        'FEDERATED STATES OF MICRONESIA': 'MICRONESIA, FEDERATED STATES OF',
        'SOUTH KOREA': 'KOREA, REPUBLIC OF',
        'BOLIVIA': 'BOLIVIA, PLURINATIONAL STATE OF',
        'TANZANIA': 'TANZANIA, UNITED REPUBLIC OF',
        'PALESTINE': 'PALESTINE, STATE OF',
    }
    common_region_errors = {}
    common_location_errors = {
        'Ramallah': 'Ramallah (Ram Allah)',
        'Yekaterinburg': 'Yekaterinburg (Ekaterinburg)',
    }

    def __init__(self, run_init=True):
        self.conn = None
        if run_init:
            self.init()

    def init(self, db_path=None):
        """Initialise the DB."""
        if not db_path:
            db_path = DB_PATH

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  # access query result as dict

        c = self.conn.cursor()

        c.executescript(
            '''
            CREATE TABLE IF NOT EXISTS country (
                code text,
                name text,
                PRIMARY KEY (code)
            );
            CREATE TABLE IF NOT EXISTS subdivision (
                country_code text,
                subdivision_code text,
                name text,
                PRIMARY KEY (country_code, subdivision_code)
            );
            CREATE TABLE IF NOT EXISTS location (
                country_code text,
                location_code text,
                name text,
                subdivision text,
                status text,
                iata text,
                longitude int,
                latitude int,
                remark text,
                is_port int,
                is_airport int,
                is_road_terminal int,
                is_rail_terminal int,
                is_postal_exchange_office int,
                is_border_cross int,
                PRIMARY KEY (country_code, location_code)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS location_index ON location(
                country_code,
                location_code,
                name,
                is_port,
                is_airport
            );
        '''
        )
        self.conn.commit()

    def close(self):
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_all_country(self):
        """Return all country codes."""
        c = self.conn.cursor()
        c.execute('SELECT * FROM country')
        r = c.fetchall()
        c.close()
        return r

    def get_all_subdivision(self):
        """Return all subdivisions."""
        c = self.conn.cursor()
        c.execute('SELECT * FROM subdivision')
        r = c.fetchall()
        c.close()
        return r

    def get_all_location(self):
        """Return all locations."""
        c = self.conn.cursor()
        c.execute('SELECT * FROM location')
        r = c.fetchall()
        c.close()
        return r

    def get_country_name(self, code):
        """Return None if could not found"""
        c = self.conn.cursor()
        c.execute('SELECT name FROM country WHERE code = ?', (code,))
        r = c.fetchone()
        c.close()
        return r[0] if r else None

    def get_iata_location(self, code):
        """
        IATA location may not be defined as airport
        reference : https://en.wikipedia.org/wiki/UN/LOCODE
        """
        if len(code) != 3:
            raise ValueError

        c = self.conn.cursor()
        c.execute(
            """
            SELECT country_code, location_code, name
            FROM location
            WHERE (location_code=? AND is_airport=1) OR (iata=? AND is_airport=1)
            """,
            (code, code),
        )
        r = c.fetchall()
        c.close()
        return r

    def get_location_name(self, country_code, location_code):
        """return None if could not found"""
        c = self.conn.cursor()
        c.execute(
            'SELECT name FROM location WHERE country_code = ? AND location_code = ?',
            (country_code, location_code),
        )
        r = c.fetchone()
        c.close()
        return r[0] if r else None

    def search_country_name(self, name):
        """return [] if could not found"""
        name = name.upper()
        name = self.common_region_errors.get(name, name)

        c = self.conn.cursor()
        c.execute('SELECT * FROM country WHERE name = ?', (name,))
        ret = c.fetchall()
        c.close()
        return ret

    def search_country_name_like(self, name):
        """return [] if could not found"""
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM country WHERE name LIKE "%%{name}%%"')
        ret = c.fetchall()
        c.close()
        return ret

    def search_country_region_name(self, country_code, name):
        """return [] if could not found"""
        name = name.upper()
        name = self.common_region_errors.get(name, name)

        c = self.conn.cursor()
        c.execute(
            'SELECT * FROM subdivision WHERE country_code = ? and name = ? COLLATE NOCASE',
            (country_code, name),
        )
        ret = c.fetchall()
        c.close()
        return ret

    def search_country_region_location_name(self, country_code, region_code, name):
        """return [] if could not found"""
        # Bug everywhere elses we do the following first
        # name = name.upper()
        name = self.common_location_errors.get(name, name)

        c = self.conn.cursor()
        if region_code:
            c.execute(
                '''
                    SELECT * FROM location WHERE
                    country_code = ? and subdivision = ? and name LIKE ?
                    COLLATE NOCASE
                    ''',
                (country_code, region_code, f'{name}%'),
            )
        else:
            c.execute(
                '''
                    SELECT * FROM location
                    WHERE country_code = ? and name LIKE ?
                    COLLATE NOCASE''',
                (country_code, f'{name}%'),
            )
        ret = c.fetchall()
        c.close()
        return ret

    def search_location_name_like(self, name):
        """return [] if could not found"""
        c = self.conn.cursor()
        name = name.replace("'", "''")
        c.execute("SELECT * FROM location WHERE name LIKE '%%{name}%%'")
        ret = c.fetchall()
        c.close()
        return ret

    def iata_to_locode(self, iata, country_code=None):
        """Convert an IATA code to a UN/Locode."""
        c = self.conn.cursor()
        if country_code:
            c.execute(
                'SELECT * FROM location WHERE location_code = ? and country_code = ?',
                (
                    iata.upper(),
                    country_code.upper(),
                ),
            )
        else:
            c.execute('SELECT * FROM location WHERE location_code = ?', (iata.upper(),))
        r = c.fetchone()
        c.close()
        return '{r[0]}-{r[1]}'.lower() if r else None

    def search_coordinates_airport(self, latitude, longitude, country_code):
        """search for an aiport based on coordinates"""
        # http://stackoverflow.com/questions/3695224/sqlite-getting-nearest-locations-with-latitude-and-longitude
        fudge = math.pow(math.cos(math.radians(latitude)), 2)
        order_by = (
            f'(({latitude} - latitude) * ({latitude} - latitude) +'
            f'({longitude} - longitude) * ({longitude} - longitude) * {fudge})'
        )
        c = self.conn.cursor()
        c.execute(
            f'''
            SELECT * FROM location
            WHERE is_airport = 1 and country_code = ?
            ORDER BY {order_by} LIMIT 1
            ''', (country_code,),
        )
        r = c.fetchall()
        c.close()
        return r if r else None

    def search_coordinates_postal(self, latitude, longitude, country_code):
        """search for an location based on coordinates"""
        fudge = math.pow(math.cos(math.radians(latitude)), 2)
        order_by = (
            f'(({latitude} - latitude) * ({latitude} - latitude) +'
            f'({longitude} - longitude) * ({longitude} - longitude) * {fudge})'
        )
        c = self.conn.cursor()
        c.execute(
            f'''
            SELECT * FROM location
            WHERE is_postal_exchange_office = 1 and country_code = ?
            ORDER BY {order_by} LIMIT 1
            ''', (country_code.upper(),),
        )
        r = c.fetchall()
        c.close()
        return r if r else None

    def search_coordinates_port(self, latitude, longitude, country_code):
        """search for an location based on coordinates"""
        fudge = math.pow(math.cos(math.radians(latitude)), 2)
        # TODO: move this to a function to make DRY
        order_by = (
            f'(({latitude} - latitude) * ({latitude} - latitude) +'
            f'({longitude} - longitude) * ({longitude} - longitude) * {fudge})'
        )
        c = self.conn.cursor()
        c.execute(
            f'''
            SELECT * FROM location
            WHERE is_port = 1 and country_code = ?
            ORDER BY {order_by} LIMIT 1
            ''', (country_code.upper(),),
        )
        r = c.fetchall()
        c.close()
        return r if r else None

    def search_coordinates(self, latitude, longitude, country_code):
        """search for an location based on coordinates"""
        fudge = math.pow(math.cos(math.radians(latitude)), 2)
        order_by = (
            f'(({latitude} - latitude) * ({latitude} - latitude) +'
            f'({longitude} - longitude) * ({longitude} - longitude) * {fudge})'
        )
        c = self.conn.cursor()
        c.execute(
            f'SELECT * FROM location WHERE country_code = ? ORDER BY {order_by} LIMIT 1',
            (country_code.upper(),),
        )
        r = c.fetchall()
        c.close()
        return r if r else None

    def search_port_name_like(self, name):
        """return [] if could not found"""
        c = self.conn.cursor()
        name = name.replace("'", "''")
        c.execute(
            f"""
            SELECT country_code, location_code, name, subdivision, is_port
            FROM location
            WHERE name LIKE '%%{name}%%' AND is_port=1
            """
        )
        ret = c.fetchall()
        c.close()
        return ret

    def gen_from_csv(self):
        """Generate the CSV file."""
        c = self.conn.cursor()
        p_code = parser.CodeParser()
        p_sub = parser.SubdivisionParser()
        for filename in os.listdir(CSVDIR):
            if os.path.splitext(filename)[1] != '.csv':
                continue
            if 'UNLOCODE' in filename:
                p_code.parse(c, join(CSVDIR, filename))
            elif 'Subdivision' in filename:
                p_sub.parse(c, join(CSVDIR, filename))
            else:
                print('skip unknow file : {filename}')

        self.conn.commit()
        c.close()

    def analytics(self, country=None):
        """Print some DB analytics."""
        if country:
            country_limit = f" AND country_code='{country}'"
            country_limit_where = f" WHERE country_code='{country}'"
        else:
            country_limit = ''
            country_limit_where = ''

        c = self.conn.cursor()
        c.execute('SELECT COUNT(*) FROM country')
        country_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM subdivision' + country_limit_where)
        subdivision_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM location' + country_limit_where)
        location_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM location WHERE is_airport=1' + country_limit)
        airport_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM location WHERE is_port=1' + country_limit)
        port_count = c.fetchone()[0]
        c.execute(
            'SELECT COUNT(*) FROM location WHERE is_road_terminal=1' + country_limit
        )
        road_terminal_count = c.fetchone()[0]
        c.execute(
            'SELECT COUNT(*) FROM location WHERE is_rail_terminal=1' + country_limit
        )
        rail_terminal_count = c.fetchone()[0]
        c.execute(
            'SELECT COUNT(*) FROM location WHERE is_postal_exchange_office=1'
            + country_limit
        )
        postal_exchange_office_count = c.fetchone()[0]
        c.execute(
            'SELECT COUNT(*) FROM location WHERE is_border_cross=1' + country_limit
        )
        border_cross_count = c.fetchone()[0]
        c.close()

        print('============= BEGIN =============')
        print(f'country count = {country_count}')
        if country:
            print(f'*** search country : "{country}" ***')
        print(f'subdivision count = {subdivision_count}')
        print(f'location count = {location_count}')
        print(f'port count = {port_count}')
        print(f'airport count = {airport_count}')
        print(f'road terminal count = {road_terminal_count}')
        print(f'rail terminal count = {rail_terminal_count}')
        print(f'postal exchange office count = {postal_exchange_office_count}')
        print(f'border cross count = {border_cross_count}')
        print('============= END =============')
