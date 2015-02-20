archiver
========

Utility to perform archiving and curation of NGS data.

Aims
----

The initial aim is to automate each stage so that it is easy to return
to a partially processed directory and be able to review and pick up
again with minimal effort.

A secondary aim is to automate the process of examining data to
uncover relationships between data files, understand and annotate the
contents, and safely remove files that are no longer required.

Dependencies
------------

Depends on the `bcftbx` and `auto_process_ngs` modules.

To create test data use the `make_mock_solid_dir.py` utility from the
`genomics` repo.

Usage
-----

Example protocol:

Create a working copy:

    archiver stage solid0123_20111014_FRAG_BC staging

Examine primary data:

    archiver primary_data staging/solid0123_20111014_FRAG_BC

Compress primary data files using `bzip2`:

    archiver compress staging/solid0123_20111014_FRAG_BC

(Other commands still to be implemented and