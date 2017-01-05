
import numpy as np
import shutil
import tempfile
import unittest

import sys
sys.path.append("..")

from format import Array


class MockDataset(object):
    def __init__(self):
        self.path = tempfile.mkdtemp()
        self.mode = "r+"
        self.is_open = True
        self.is_writable = True

    def close(self):
        shutil.rmtree(self.path)

    def _check_open(self):
        assert self.is_open

    def _check_writable(self):
        assert self.is_writable


class ArrayTest(unittest.TestCase):
    def setUp(self):
        self.dataset = MockDataset()

    def tearDown(self):
        self.dataset.close()

    def test_create(self):
        dset = self.dataset
        data = np.random.rand(10, 2)
        array = Array.create(dset, "hello", data)

        self.assertTrue(isinstance(array, Array))
        self.assertEqual(
            (10, 2),
            array[:].shape
        )
        self.assertTrue(np.all(array[:] == data))

        array = Array(dset, "hello")
        self.assertEqual(
            (10, 2),
            array[:].shape
        )
        self.assertTrue(np.all(array[:] == data))

    def test_attributes(self):
        dset = self.dataset
        data = np.random.rand(10, 2)
        array = Array.create(dset, "hello", data)

        self.assertTrue(array.attributes is None)

        array.attributes = {
            "category": "Foobar",
            "path": "More foobar"
        }

        self.assertTrue("category" in array.attributes)

        array = Array(dset, "hello")
        self.assertEqual("Foobar", array.attributes["category"])

    def test_slice(self):
        dset = self.dataset
        data = np.random.rand(10, 2)
        array = Array.create(dset, "hello", data)
        self.assertTrue(np.all(array[3:5, 1] == data[3:5, 1]))

        array = Array(dset, "hello")
        self.assertTrue(np.all(array[3:5] == data[3:5]))

        array[3:5] = 0
        array = Array(dset, "hello")
        self.assertTrue(np.all(array[3:5] == 0))

    def test_writable(self):
        dset = self.dataset
        dset.is_writable = False
        with self.assertRaises(AssertionError):
            array = Array.create(dset, "hello", np.random.rand(10, 2))

    def test_open(self):
        dset = self.dataset
        array = Array.create(dset, "hello", np.random.rand(10, 2))
        dset.is_open = False
        with self.assertRaises(AssertionError):
            array[:].shape


if __name__ == "__main__":
    unittest.main()
