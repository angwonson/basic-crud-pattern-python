from shutil import rmtree
import uuid
from testify import TestCase, setup, assert_equal, class_setup, teardown
from lib.model.keycolumnvaluestore import KeyColumnValueStore
from collections import OrderedDict


class TestKeyColumnValueStore(TestCase):

    @class_setup
    def class_detup(self):
        """ set up default directory for data """
        self.disk_path_base = "/tmp/basic-crud-python-data/"

    @teardown
    def teardown(self):
        """ delete files on disk from completed test """
        rmtree(self.disk_path)

    @setup
    def setup_store_and_defaults(self):
        self.myuuid = str(uuid.uuid4())
        self.disk_path = self.disk_path_base + self.myuuid
        self.store = KeyColumnValueStore(self.disk_path)
        self.store.set('a', 'ab', 'xb')
        self.store.set('a', 'aa', 'xa')
        self.store.set('c', 'cd', 'xd')
        self.store.set('c', 'cc', 'xc')
        self.store.set('d', 'df', 'xf')
        self.store.set('d', 'de', 'xe')

    def test_set(self):
        j = self.store.set('a', 'aa', 'x')
        assert_equal(j, True)

    def test_get(self):
        j = self.store.get('a', 'aa')
        assert_equal(j, 'xa')

    def test_get_key(self):
        j = self.store.get_key('a')
        assert_equal(j, OrderedDict([('aa', 'xa'), ('ab', 'xb')]).items())

    def test_get_keys(self):
        j = self.store.get_keys()
        assert_equal(j, set(['a', 'c', 'd']))

    def test_get_keys_empty(self):
        k_myuuid = str(uuid.uuid4())
        k_disk_path = self.disk_path_base + k_myuuid
        new_store = KeyColumnValueStore(k_disk_path)
        k = new_store.get_keys()
        assert_equal(k, set([]))

    def test_get_nonexistent(self):
        j = self.store.get('z', 'yy')
        assert_equal(j, None)

    def test_get_nonexistent_key(self):
        j = self.store.get_key('z')
        assert_equal(j, [])

    def test_delete(self):
        j = self.store.get('a', 'aa')
        assert_equal(j, 'xa')
        j = self.store.delete('a', 'aa')
        assert_equal(j, True)
        k = self.store.get('a', 'aa')
        assert_equal(k, None)

    def test_delete_key(self):
        j = self.store.get('a', 'aa')
        assert_equal(j, 'xa')
        j = self.store.delete_key('a')
        assert_equal(j, True)
        k = self.store.get('a', 'aa')
        assert_equal(k, None)

    def test_change_values(self):
        self.store.set('a', 'aa', 'y')
        self.store.set('a', 'ab', 'z')
        j = self.store.get('a', 'aa')
        assert_equal(j, 'y')
        k = self.store.get_key('a')
        expected_result = OrderedDict([('aa', 'y'), ('ab', 'z')]).items()
        assert_equal(k, expected_result)
