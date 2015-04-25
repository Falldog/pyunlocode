pyunlocode
====================
Python wrap module for UN/LOCODE

Version
-------------------
0.9

UN/LOCODE data
-------------------
* from [UN ECE]

Implementation
-------------------
* parse CVS files and store into sqlite DB
* store the country/city in UTF-8 encoding

Usage
-------------------
* initialize
```sh
$ python pyunlocode.py
```
* query city or country name by code
```python
import pyunlocode
u = pyunlocode.PyUnLocode()
u.init()
print u.get_country_name('US')
print u.get_city_name('TWTPE')
u.close()
```


[UN ECE]: http://www.unece.org/cefact/codesfortrade/codes_index.html
