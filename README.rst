Simple Dataset
==============

This library aims to provide a simple file format to read and write big
datasets of large numpy arrays with seamless compression. This format aims to
duplicate part of the hdf5 functionality using the host file system.

Features
--------

* Random access across all arrays
* Metadata for all arrays

Missing Features
----------------

* A single array **can not** be larger than the available memory

Example
-------

.. code:: python

    import numpy as np

    from simple_dataset import Dataset

    with Dataset("/path/to/dataset") as dset:
        arr = dset.create_array("test", np.random.rand(1000, 128))
        arr.attributes = {"message": "Just a random array"}
        # any slice returns the underlying numpy array
        arr[:] = (arr[:]-0.5)*2  # saved back to the file

    with Dataset("/path/to/dataset") as dset:
        assert "test" in dset
        print dset["test"][:].mean()  # Should be very close to 0

    with Dataset("/path/to/dataset") as dset:
        # Ellipsis can be used to save a completely new array
        dset["test"][...] = np.random.rand(10, 2)
