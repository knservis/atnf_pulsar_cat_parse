# This should not be an issue in any modern computer, but it does read the whole file into memory
from cStringIO import StringIO
import requests
import tarfile
import struct
import json

def get_record(atnf_list):
    g = []
    for el in atnf_list:
        if el.startswith('@----'):
            yield g
        g.append(el)
    yield g

dict_field_labels = ['key', 'value', 'error', 'reference']
fstr = '9s 25s 5s 12s'
fstruct = struct.Struct(fstr)
dict_parser = fstruct.unpack_from

def dict_parse(dict_string):
    return dict(zip(dict_field_labels, [ x.strip() for x in dict_parser(dict_string)]))

def array_parse(array_string):
    kv = array_string.split()
    return {'key': kv[0], 'value': kv[1].strip().split(',')}


file = 'psrcat_tar/psrcat.db'
url = 'http://www.atnf.csiro.au/people/pulsar/psrcat/downloads/psrcat_pkg.tar.gz'

response = requests.get(url)

tar = tarfile.open(mode= 'r:gz', fileobj = StringIO(response.content))

db = tar.extractfile(file)

cont = db.readlines()

res = []
for rec in get_record(cont):
    rec_dict = {}
    for field in rec:
        if field.startswith('#') or field.startswith('@'):
            continue
        decoded_field = {}
        if field.startswith('SURVEY') or field.startswith('ASSOC') or field.startswith('TYPE'):
            decoded_field = array_parse(field)
            rec_dict[decoded_field['key']] = decoded_field['value']
        else:
            decoded_field = dict_parse(field)
            rec_dict[decoded_field['key']] = dict(filter(lambda x: x != 'key', decoded_field.items()))

    res.append(rec_dict)

with open('atnf_pulsarcat.json', 'w') as f:
    f.write(json.dumps(res))