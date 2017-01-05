
import errno
import gzip
import numpy as np
import os
from os import path
import pickle
import shutil


class Dataset(object):
    """Dataset represents a dataset written on disk with the simple_dataset
    format.
    
    Parameters
    ----------
    dataset_path : string
                   The path to save the dataset in
    mode : {"r", "w", "r+"}
           The mode to open the dataset with (default: "r+")
    """
    def __init__(self, dataset_path, mode="r+"):
        assert mode in {"r", "w", "r+"}, "Mode should be in {'r', 'w', 'r+'}"

        self.path = path.realpath(dataset_path)
        self.mode = mode
        self.lock_path = path.join(self.path, ".lock")
        self.contents_path = path.join(self.path, "contents.db")

        # Cannot read what's not there
        if not path.exists(self.path) and self.mode == "r":
            raise IOError("No such file or directory: '%s'" % (self.path,))

        # Try to create it if it is not there
        empty = False
        try:
            os.mkdir(self.path)
        except OSError as e:
            if e.errno == errno.EEXIST:    # File exists
                pass  # This is expected behaviour
            elif e.errno == errno.ENOENT:  # File not found
                raise IOError("No such file or directory: '%s'" % (self.path,))
            else:
                raise
        
        # Try to lock it
        try:
            os.mkdir(self.lock_path)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise IOError(("Can't lock the dataset. If no other process "
                               "is using it, manually delete the '.lock' "
                               "directory"))
            else:
                raise

        # Empty the dataset if the mode is 'w'
        if self.mode == "w":
            for item in os.listdir(self.path):
                shutil.rmtree(path.join(self.path, item))

        # Finally set the closed attribute to False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def __del__(self):
        self.close()

    def _check_open(self):
        assert not self.closed, "The dataset is closed"

    def _check_writable(self):
        assert not self.closed and self.mode in {"w", "r+"}, ("The dataset is "
                                                              "closed or not "
                                                              "writable")

    def close(self):
        if path.exists(self.lock_path):
            os.rmdir(self.lock_path)
        self.closed = True

    def _list_keys(self, prefix=""):
        return filter(
            lambda x: x != ".lock" and not x.endswith(".pickle"),
            os.listdir(path.join(self.path, prefix))
        )

    def keys(self):
        return self._list_keys()

    def __contains__(self, key):
        return key in self.keys()

    def __getitem__(self, key):
        self._check_open()

        if not isinstance(key, basestring):
            raise TypeError("Dataset accepts only string keys")

        object_path = path.join(self.path, key)
        if not path.exists(object_path):
            raise KeyError("'%s' not found in Dataset" % (key,))
        elif path.isdir(object_path):
            return Group(self, key)
        elif path.isfile(object_path):
            return Array(self, key)
        else:
            raise KeyError("'%s' not found in Dataset" % (key,))

    def __delitem__(self, key):
        self._check_writable()

        if not isinstance(key, basestring):
            raise TypeError("Dataset accepts only string keys")

        object_path = path.join(self.path, key)
        if path.exists(object_path):
            if path.isdir(object_path):
                shutil.rmtree(object_path)
            elif path.isfile(object_path):
                os.unlink(object_path)
                try:
                    os.unlink(object_path + ".pickle")
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise

    def create_group(self, key):
        self._check_writable()

        try:
            os.makedirs(path.join(self.path, key))
        except OSError as e:
            if (
                e.errno != errno.EEXIST or
                path.isfile(path.join(self.path, key))
            ):
                raise

        return self[key]

    def create_array(self, key, array):
        if path.dirname(key) != "":
            group = self.create_group(path.dirname(key))
        return Array.create(self, key, array)


class Group(object):
    """A group of arrays in a dataset.
    
    Parameters
    ----------
    dataset : Dataset
              The dataset this group belongs to
    key : string
          The group fully qualified path from root
    """
    def __init__(self, dataset, key):
        self.dataset = dataset
        self.key = key

    def keys(self):
        return self.dataset._list_keys(self.key)

    def __contains__(self, key):
        return key in self.keys()

    def __getitem__(self, key):
        return self.dataset[path.join(self.key, key)]

    def __delitem__(self, key):
        del self.dataset[path.join(self.key, key)]

    def create_group(self, key):
        return self.dataset.create_group(path.join(self.key, key))

    def create_array(self, key, array):
        return self.dataset.create_array(path.join(self.key, key), array)

class Array(object):
    """A single array in a dataset.
    
    Parameters
    ----------
    dataset : Dataset
              The dataset this array belongs to
    key : string
          The fully qualified path from root for this array
    """
    def __init__(self, dataset, key):
        self.dataset = dataset
        self.key = key
        self.data = None

    @property
    def array_path(self):
        return path.join(self.dataset.path, self.key)

    @property
    def attributes_path(self):
        return self.array_path + ".pickle"

    @property
    def attributes(self):
        """The attributes accompanying this array"""
        self.dataset._check_open()

        if not hasattr(self, "_attributes"):
            if not path.exists(self.attributes_path):
                self._attributes = None
            else:
                with open(self.attributes_path) as f:
                    self._attributes = pickle.load(f)
        return self._attributes

    @attributes.setter
    def attributes(self, new_attributes):
        self.dataset._check_writable()

        with open(self.attributes_path, "w") as f:
            pickle.dump(new_attributes, f, protocol=2)
        self._attributes = new_attributes

    def __getitem__(self, key):
        self.dataset._check_open()

        if self.data is None:
            with gzip.open(self.array_path) as f:
                self.data = np.load(f)
        return self.data[key]

    def __setitem__(self, key, value):
        self.dataset._check_writable()

        tmpdata = self[key]
        tmpdata[:] = value
        with gzip.open(self.array_path, "w") as f:
            np.save(f, self.data)

    @classmethod
    def create(cls, dataset, key, array):
        dataset._check_writable()
        
        arr = cls(dataset, key)
        with gzip.open(arr.array_path, "w") as f:
            np.save(f, array)

        return arr
