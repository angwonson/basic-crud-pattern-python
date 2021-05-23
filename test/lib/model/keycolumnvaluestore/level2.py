import uuid
from shutil import rmtree
from testify import TestCase, setup, assert_equal, class_setup, teardown
from lib.model.keycolumnvaluestore import KeyColumnValueStore


class TestKeyColumnValueStore(TestCase):

    @class_setup
    def class_setup(self):
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
        self.store.set('a', 'aa', 'xa')
        self.store.set('a', 'ab', 'xb')
        self.store.set('a', 'ac', 'xc')
        self.store.set('a', 'ad', 'xd')
        self.store.set('a', 'ae', 'xe')
        self.store.set('a', 'af', 'xf')
        self.store.set('a', 'ag', 'xg')

    def test_get_slice(self):
        j2 = self.store.get_slice('a', 'ac', 'ae')
        k2 = self.store.get_slice('a', 'ae', None)
        l2 = self.store.get_slice('a', None, 'ac')
        assert_equal(j2, [('ac', 'xc'), ('ad', 'xd'), ('ae', 'xe')])
        assert_equal(k2, [('ae', 'xe'), ('af', 'xf'), ('ag', 'xg')])
        assert_equal(l2, [('aa', 'xa'), ('ab', 'xb'), ('ac', 'xc')])

    def test_get_slice_undefined(self):
        m = self.store.get_slice('b', None, None)
        assert_equal(m, [])
