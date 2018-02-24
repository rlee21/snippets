Avvo Workflow Deploy
====================

Avvo Workflow Deploy is a python package for packaging, deploying, and running
oozie workflows in the test or prod environments.

Installation:
-------------

This package lives in our internal PyPI. To install you must update your
pip.conf to use the extra index in packagecloud. Once your pip config is
updated you can run the following command:

.. code-block:: sh

    pip install wdep
    ...

Example Usage:
--------------

.. code-block:: sh

    wdep package
    ...

