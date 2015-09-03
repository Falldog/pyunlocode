pyunlocode
====================
Python wrap module for UN/LOCODE

Version
-------------------
0.9

UN/LOCODE data
-------------------
* data from [UN ECE]
* reference data from CSV "{yyyy}-{v} SubdivisionCodes.csv" & "{yyyy}-{v} UNLOCODE CodeListPart?.csv"

Implementation
-------------------
* parse CSV files and store into sqlite DB
* store the country/subdivision/location in UTF-8 encoding

Usage
-------------------
* initialize sqlite DB
```sh
$ python pyunlocode.py
```
* query city or country name by code
```python
import pyunlocode
u = pyunlocode.PyUnLocode()
u.init()
print u.get_country_name('US')
print u.get_city_name('TW', 'TPE')
u.close()
```

SQLite Table
-------------------
* country

| field    | type      |
|----------|-----------|
| code     | text (PK) |
| name     | text      |

* subdivision

| field            | type      |
|------------------|-----------|
| country_code     | text (PK) |
| subdivision_code | text (PK) |
| name             | text      |

* location

| field                     | type      |
|---------------------------|-----------|
| country_code              | text (PK) |
| location_code             | text (PK) |
| name                      | text      |
| subdivision               | text      |
| status                    | text      |
| iata                      | text      |
| coordinate                | text      |
| remark                    | text      |
| is_port                   | int       |
| is_airport                | int       |
| is_road_terminal          | int       |
| is_rail_terminal          | int       |
| is_postal_exchange_office | int       |
| is_border_cross           | int       |

[UN ECE]: http://www.unece.org/cefact/codesfortrade/codes_index.html
