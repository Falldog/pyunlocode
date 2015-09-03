#!/usr/bin/env python
# 
# Create on : 2015/04/19
#
# @author : Falldog
#
import os
import parser
import sqlite3
from os.path import join


CURDIR = os.path.abspath(os.path.dirname(__file__))
CSVDIR = join(CURDIR, 'csv')
DB_PATH = join(CURDIR, 'unlocode.db')


class PyUnLocode():
    """
    Download from : http://www.unece.org/cefact/codesfortrade/codes_index.html
    Column Spec : http://www.unece.org/fileadmin/DAM/cefact/locode/Service/LocodeColumn.htm
    """
    def __init__(self):
        self.conn = None

    def init(self, db_path=None):
        if not db_path:
            db_path = DB_PATH

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  # access query result as dict

        c = self.conn.cursor()

        c.executescript('''
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
                coordinate text,
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
        ''')
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_all_country(self):
        c = self.conn.cursor()
        c.execute('SELECT * FROM country')
        r = c.fetchall()
        c.close()
        return r

    def get_all_subdivision(self):
        c = self.conn.cursor()
        c.execute('SELECT * FROM subdivision')
        r = c.fetchall()
        c.close()
        return r

    def get_all_location(self):
        c = self.conn.cursor()
        c.execute('SELECT * FROM location')
        r = c.fetchall()
        c.close()
        return r

    def get_country_name(self, code):
        """ return None if could not found """
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
        c.execute("""
            SELECT country_code, location_code, name
            FROM location
            WHERE (location_code=? AND is_airport=1) OR (iata=? AND is_airport=1)
            """,
            (code, code))
        r = c.fetchall()
        c.close()
        return r

    def get_location_name(self, country_code, location_code):
        """ return None if could not found """
        c = self.conn.cursor()
        c.execute('SELECT name FROM location WHERE country_code = ? AND location_code = ?', (country_code, location_code))
        r = c.fetchone()
        c.close()
        return r[0] if r else None

    def search_country_name_like(self, name):
        """ return [] if could not found """
        c = self.conn.cursor()
        c.execute('SELECT * FROM country WHERE name LIKE "%%%s%%"' % name)
        ret = c.fetchall()
        c.close()
        return ret

    def search_location_name_like(self, name):
        """ return [] if could not found """
        c = self.conn.cursor()
        name = name.replace("'", "''")
        c.execute("SELECT * FROM location WHERE name LIKE '%%%s%%'" % name)
        ret = c.fetchall()
        c.close()
        return ret

    def search_port_name_like(self, name):
        """ return [] if could not found """
        c = self.conn.cursor()
        name = name.replace("'", "''")
        c.execute("""
            SELECT country_code, location_code, name, subdivision, is_port
            FROM location
            WHERE name LIKE '%%%s%%' AND is_port=1
        """ % name)
        ret = c.fetchall()
        c.close()
        return ret

    def gen_from_csv(self):
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
                print 'skip unknow file : %s' % filename

        self.conn.commit()
        c.close()

    def analytics(self, country=None):
        if country:
            country_limit = " AND country_code='%s'" % country
            country_limit_where = " WHERE country_code='%s'" % country
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
        c.execute('SELECT COUNT(*) FROM location WHERE is_road_terminal=1' + country_limit)
        road_terminal_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM location WHERE is_rail_terminal=1' + country_limit)
        rail_terminal_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM location WHERE is_postal_exchange_office=1' + country_limit)
        postal_exchange_office_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM location WHERE is_border_cross=1' + country_limit)
        border_cross_count = c.fetchone()[0]
        c.close()

        print '============= BEGIN ============='
        print 'country count = %d' % country_count
        if country:
            print '*** search country : "%s" ***' % country
        print 'subdivision count = %d' % subdivision_count
        print 'location count = %d' % location_count
        print 'port count = %d' % port_count
        print 'airport count = %d' % airport_count
        print 'road terminal count = %d' % road_terminal_count
        print 'rail terminal count = %d' % rail_terminal_count
        print 'postal exchange office count = %d' % postal_exchange_office_count
        print 'border cross count = %d' % border_cross_count
        print '============= END ============='


def main():
    try:
        u = PyUnLocode()
        u.init()
        u.gen_from_csv()
        u.analytics()
        u.analytics('TW')
        print u.get_country_name('US')
        print u.get_location_name('TW', 'TPE')
        r = u.search_location_name_like('LOS ANGELES')
        for c in r:
            print "code:%s%s name:%s" % (c['country_code'], c['location_code'], c['name'])
        u.close()

    except:
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()

