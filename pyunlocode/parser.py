#!/usr/bin/env python
#
# Create on : 2015/08/31
#
# @author : Falldog
#
import csv

ENCODING = 'latin-1'  # ISO-8859-1
DELIMITER = ','
QUOT_CHAR = '"'

# status : description
STATUS_CODE = {
    'AA': "Approved by competent national government agency",
    'AC': "Approved by Customs Authority",
    'AF': "Approved by national facilitation body",
    'AI': "Code adopted by international  organisation (IATA, ECLAC, EUROSTAT, etc.)",
    'AM': "Approved by the UN/LOCODE Maintenance Agency",
    'AQ': "Entry approved, functions not verified",
    'AS': "Approved by national standardisation body",
    'QQ': "Original entry not verified since date indicated",
    'RL': "Recognised location - Existence and representation of location name confirmed by check against nominated gazetteer or other reference work",
    'RN': "Request from credible national sources for locations in their own country",
    'RQ': "Request under consideration",
    'RR': "Request rejected",
    'UR': "Entry included on user's request; not officially approved",
    'XX': "Entry that will be removed from the next issue of UN/LOCODE",
}


def boolean(b):
    """ for SQLite boolean used """
    return '1' if b else '0'


class SubdivisionParser(object):
    def __init__(self):
        pass

    def parse(self, cursor, filepath):
        """
        Columns :
            CountryCode, SubdivisionCode, SubdivisionName, Type?
        """
        with open(filepath, 'rb') as f:
            data_reader = csv.reader(f, delimiter=DELIMITER, quotechar=QUOT_CHAR)
            for row in data_reader:
                country_code, subdivision_code, subdivision_name, _type = row
                subdivision_name = subdivision_name.decode(ENCODING)
                cursor.execute(
                    "INSERT OR REPLACE INTO subdivision VALUES (?,?,?)",
                    (country_code,
                     subdivision_code,
                     subdivision_name,
                     )
                )


class CodeParser(object):
    def __init__(self):
        pass

    def parse(self, cursor, filepath):
        """
        Columns :
            Change, CountryCode, LocationCode, LocationName, LocationName without Diacritics,
            Subdivision, Function, Status, Date, IATA, Coordinate, Remark

        Column - Change:
            + (newly added);
            X (to be removed);
            | (changed);
            # (name changed);
            = (reference entry);
            ! (US location with duplicate IATA code)
        Column - Function:
            0 Function not known, to be specified
            1 Port, as defined in Rec 16
            2 Rail Terminal
            3 Road Terminal
            4 Airport
            5 Postal Exchange Office
            6 Multimodal Functions (ICDs, etc.)
            7 Fixed Transport Functions (e.g. Oil platform)
            8 Inland Port
            B Border Crossing
        Column - Date:
            ym
        Column - Coordinate:
            (DDMM[N/S] DDDMM[W/E])
        """

        with open(filepath, 'rb') as f:
            data_reader = csv.reader(f, delimiter=DELIMITER, quotechar=QUOT_CHAR)
            for row in data_reader:
                change = row[0]
                if change == 'X':  # skip removed item
                    continue
                elif change == '=':  # skip reference entry ex: "Peking = Beijing"
                    continue
                elif change == '\xa6':  # '|' skip non location entry
                    continue

                change, country_code, location_code, location_name, location_name_wo_diacritics, subdivision, function, status, date, iata, coordinate, remark = row

                coordinates = coordinate.split()
                longitude = 91
                latitude  = 91

                if len(coordinates) == 2:
                    latitude            = float(coordinates[0][:-1]) / 100
                    latitude_direction  = coordinates[0][-1]
                    longitude           = float(coordinates[1][:-1]) / 100
                    longitude_direction = coordinates[1][-1]
                    if latitude_direction == 'S':
                        latitude *= -1
                    if longitude_direction == 'W':
                        longitude *= -1

                if location_name and location_name[0] == '.':  # country name
                    name = location_name.decode(ENCODING)[1:]  # filter the first char "."
                    # Not sure why this is done as it looses information such as Korea, republic of
                    # name = name.split(',')[0]
                    cursor.execute(
                        "INSERT OR REPLACE INTO country VALUES (?,?)",
                        (country_code, name)
                    )

                else:  # location name
                    if not location_code:
                        print '*** skip unknow location code record : %s' % row
                        continue

                    remark = remark.decode(ENCODING)
                    is_port = '1' in function
                    is_airport = '4' in function
                    is_rail_terminal = '2' in function
                    is_road_terminal = '3' in function
                    is_postal_exchange_office = '5' in function
                    is_border_cross = 'B' in function

                    # insert by replace, or will cause primary key conflict exception
                    # most case is alternative name switch (ONLY)
                    # Ex:
                    #   AX MHQ : "Maarianhamina (Mariehamn)" vs "Mariehamn (Maarianhamina)"
                    #   BE BTS : "Bassenge (Bitsingen)" vs "Bitsingen (Bassenge)"
                    #
                    # rarely case maybe all different
                    # Ex:
                    #   ,"US","LEB","Hanover-Lebanon-White River Apt","Hanover-Lebanon-White River Apt","NH","--34----","AI","0307",,"4338N 07215W",
                    #   ,"US","LEB","Lebanon-White River-Hanover Apt","Lebanon-White River-Hanover Apt","VT","---4----","AI","9601",,,
                    #   ,"US","LEB","White River-Hanover-Lebanon Apt","White River-Hanover-Lebanon Apt","VT","---4----","AI","0001",,,
                    #
                    # for these rarely case replace by last record
                    cursor.execute(
                        "INSERT OR REPLACE INTO location VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (country_code,
                         location_code,
                         location_name_wo_diacritics,
                         subdivision,
                         status,
                         iata,
                         longitude,
                         latitude,
                         remark,
                         boolean(is_port),
                         boolean(is_airport),
                         boolean(is_road_terminal),
                         boolean(is_rail_terminal),
                         boolean(is_postal_exchange_office),
                         boolean(is_border_cross),
                         )
                    )


