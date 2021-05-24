from shutil import rmtree
from os import remove
import glob
from distutils.dir_util import mkpath
from collections import OrderedDict
import simplejson as json


class KeyColumnValueStore(object):

    def __init__(self, mypath):
        self.disk_path = mypath
        self.datastore = dict()
        self._get_data_from_disk()

    def _get_data_from_disk(self):
        """ load up data from disk when application starts"""
        """ would be better to use something like couchbase here! """
        files_to_search = self.disk_path + "/*/*/*.json"
        json_files = glob.glob(files_to_search)
        for filename in json_files:
            filename_parts = filename.split("/")
            mykeyname = filename_parts[4]
            colname_parts = filename_parts[5].split(".json")
            mycolname = colname_parts[0]
            with open(filename, "r") as text_file:
                raw_data = text_file.read()
                dict_from_json = json.loads(raw_data)
                """text_file.write(output_json)"""
                if mykeyname not in self.datastore:
                    self.datastore[mykeyname] = dict()
                self.datastore[mykeyname][mycolname] = dict_from_json

    def _get_filename_base(self, key):
        key_firstletter = key[0]
        filename_base = self.disk_path + '/' + key_firstletter + '/' + key
        return filename_base

    def _get_filename(self, key, col):
        filename_base = self._get_filename_base(key)
        mkpath(filename_base)
        write_filename = filename_base + "/" + col + ".json"
        return write_filename

    def _save_to_disk(self, key, col):
        if key not in self.datastore:
            return False
        if col not in self.datastore[key]:
            return False
        output_json = json.dumps(self.datastore[key][col])
        """ would be smart to check the filename for safety here"""
        write_filename = self._get_filename(key, col)
        with open(write_filename, "w") as text_file:
            text_file.write(output_json)

    def set(self, key, col, val):

        """ sets the value at the given key/column """
        if key not in self.datastore:
            self.datastore[key] = dict()
        self.datastore[key][col] = val
        """ save to disk """
        self._save_to_disk(key, col)
        return True

    def get(self, key, col):

        """ return the value at the specified key/column """
        if key not in self.datastore:
            return None
        if col not in self.datastore[key]:
            return None
        return self.datastore[key][col]

    def get_key(self, key):

        """ returns a sorted list of column/value tuples """
        """ sort by value instead? change t[0] to t[1] """
        if key not in self.datastore:
            return []
        return OrderedDict(sorted(
            self.datastore[key].items(), key=lambda t: t[0])).items()

    def get_keys(self):

        """ returns a set containing all of the keys in the store """
        mykeys = set()
        for mykey in self.datastore:
            mykeys.add(mykey)
        return mykeys

    def delete(self, key, col):

        """ removes a column/value from the given key """
        del self.datastore[key][col]
        write_filename = self._get_filename(key, col)
        remove(write_filename)
        return True

    def delete_key(self, key):

        """ removes all data associated with the given key """
        del self.datastore[key]
        filename_base = self._get_filename_base(key)
        rmtree(filename_base)
        return True

    def get_slice(self, key, start, stop):

        """
        returns a sorted list of column/value tuples where the column
        values are between the start and stop values, inclusive of the
        start and stop values. Start and/or stop can be None values,
        leaving the slice open ended in that direction
        """
        if key not in self.datastore:
            return []
        myitems = []
        for k, v in OrderedDict(sorted(self.datastore[key].items(),
                                key=lambda t: t[0])).items():
            if (
                (start is not None and stop is not None
                    and k >= start and k <= stop)
                or (start is not None and k >= start and stop is None)
                    or (start is None and stop is not None and k <= stop)):
                myitems.append((k, v))
        return myitems
