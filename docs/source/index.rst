arqvist: explore & curate NGS data
==================================

**This project is a work-in-progress**

Aims
****

``arqvist`` aims to provide tools for managing and curating NGS data
which are stored within directories.

The initial aims are:

- enable verification of data when relocating on physical storage,
  and identification of changes
- enable capture and restoration of properties such as timestamps,
  ownership and file permissions

Downstream aims include:

- checking for duplicated data
- cleaning up by compressing large files and removing files that
  are no longer required
- adding metadata to describe the data
- facilitate auditing and discovery

Installation
************

``arqvist`` depends on the ``genomics-bcftbx`` and ``auto_process_ngs``
Python modules.

To install directly from GitHub::

    pip install -r https://github.com/pjbriggs/arqvist/raw/master/requirements.txt
    pip install git+https://github.com/pjbriggs/arqvist.git

Quickstart
**********

Record properties for the current directory::

    arqv init

See which files have changed properties::

    arqv status

Find out which properties have changed::

    arqv diff

Check whether a copy of the directory differs from the source::

    arqv status -t /data/run1_copy

Dependencies
************

Depends on the ``genomics-bcftbx`` and ``auto_process_ngs`` modules.

To create test data use the ``make_mock_solid_dir.py`` utility from the
``genomics`` repo.

Protocols
*********

.. toctree::
   :maxdepth: 2

   primary_data
   analysis_products

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

