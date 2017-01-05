
import numpy as np
from os import path
import shutil
import tempfile
import unittest

import sys
sys.path.append("..")

from format import Dataset


class DatasetTest(unittest.TestCase):
    def setUp(self):
        self.tmppath = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmppath)

    def test_create(self):
        dset = Dataset(path.join(self.tmppath, "foo"))

        self.assertFalse(dset.closed)
        self.assertEqual([], dset.keys())
        self.assertFalse("bar" in dset)

        dset.close()
        self.assertTrue(dset.closed)

    def test_lock(self):
        with Dataset(path.join(self.tmppath, "foo")) as dset1:
            with self.assertRaises(IOError):
                dset2 = Dataset(path.join(self.tmppath, "foo"))
            dset3 = Dataset(path.join(self.tmppath, "bar"))

    def test_read_non_existent(self):
        with self.assertRaises(IOError):
            Dataset(path.join(self.tmppath, "foo"), "r")

    def test_truncate_on_open(self):
        with Dataset(path.join(self.tmppath, "foo")) as dset:
            dset.create_group("bar")
            self.assertEqual(["bar"], dset.keys())

        with Dataset(path.join(self.tmppath, "foo"), "w") as dset:
            self.assertEqual([], dset.keys())

    def test_create_group(self):
        with Dataset(path.join(self.tmppath, "foo")) as dset:
            group = dset.create_group("foo")
            self.assertEqual(["foo"], dset.keys())
            self.assertEqual(group.key, dset["foo"].key)
            group2 = group.create_group("bar")
            self.assertEqual("foo/bar", group2.key)

        with Dataset(path.join(self.tmppath, "foo")) as dset:
            self.assertEqual(["foo"], dset.keys())
            self.assertEqual(["bar"], dset["foo"].keys())
            self.assertEqual("foo/bar", dset["foo/bar"].key)

        with Dataset(path.join(self.tmppath, "foo")) as dset:
            group = dset.create_group("foobar/baz/bam")
            self.assertEqual("foobar/baz/bam", group.key)

    def test_create_array(self):
        data = np.random.rand(10, 2)
        with Dataset(path.join(self.tmppath, "foo")) as dset:
            array = dset.create_array("bar", data)
            self.assertEqual(["bar"], dset.keys())
            self.assertEqual(data.shape, array[:].shape)
            self.assertTrue(np.all(data == array[:]))

        with Dataset(path.join(self.tmppath, "foo")) as dset:
            self.assertEqual(["bar"], dset.keys())
            self.assertEqual(data.shape, dset["bar"][:].shape)
            self.assertTrue(np.all(data == dset["bar"][:]))
            dset["bar"][:] = 0

        with Dataset(path.join(self.tmppath, "foo")) as dset:
            self.assertEqual(["bar"], dset.keys())
            self.assertEqual(data.shape, dset["bar"][:].shape)
            self.assertTrue(np.all(0 == dset["bar"][:]))

        with Dataset(path.join(self.tmppath, "foo")) as dset:
            array = dset.create_array("baz/bam/boo", data)
            self.assertTrue("baz" in dset)
            self.assertTrue("bam" in dset["baz"])
            self.assertTrue("boo" in dset["baz/bam"])

    def test_delete(self):
        data = np.random.rand(10, 2)
        with Dataset(path.join(self.tmppath, "foo")) as dset:
            dset.create_array("bar", data)
            dset.create_group("foo")
            self.assertEqual(["bar", "foo"], sorted(dset.keys()))

        with Dataset(path.join(self.tmppath, "foo")) as dset:
            del dset["bar"]
            self.assertTrue("bar" not in dset)

        with Dataset(path.join(self.tmppath, "foo")) as dset:
            self.assertTrue("bar" not in dset)
            del dset["bar"]
            del dset["foo"]
            self.assertEqual([], dset.keys())


if __name__ == "__main__":
    unittest.main()
