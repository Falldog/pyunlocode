#!/usr/bin/env python3
from . import PyUnLocode


def main():
    try:
        u = PyUnLocode()
        u.init()
        u.gen_from_csv()
        u.analytics()
        u.analytics('TW')
        print(u.get_country_name('US'))
        print(u.get_location_name('TW', 'TPE'))
        r = u.search_location_name_like('LOS ANGELES')
        for c in r:
            print(f"code:{c['country_code']}{c['location_code']} name:{c['name']}")
        u.close()

    except:  # noqa
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
