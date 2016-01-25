:mod:`windmill.schedulers.blocking`
======================================

.. automodule:: windmill.schedulers.blocking

API
---

.. autoclass:: BlockingScheduler
    :show-inheritance:


Introduction
------------

BlockingScheduler is the simplest possible scheduler. It runs in the foreground, so when you call
:meth:`~windmill.schedulers.blocking.BlockingScheduler.start`, the call never returns.

BlockingScheduler can be useful if you want to use hoi as a standalone scheduler (e.g. to build a daemon).

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~windmill.executors.pool.PoolExecutor`
   * - External dependencies
     - none
   * - Example
     - ``examples/schedulers/blocking.py``
       (`view online <https://bitbucket.org/agronholm/hoi/src/master/examples/schedulers/blocking.py>`_).
