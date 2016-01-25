:mod:`windmill.schedulers.background`
========================================

.. automodule:: windmill.schedulers.background

API
---

.. autoclass:: BackgroundScheduler
    :show-inheritance:


Introduction
------------

BackgroundScheduler runs in a thread **inside** your existing application. Calling
:meth:`~windmill.schedulers.blocking.BackgroundScheduler.start` will start the scheduler and it will continue running
after the call returns.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~windmill.executors.pool.PoolExecutor`
   * - External dependencies
     - none
   * - Example
     - ``examples/schedulers/background.py``
       (`view online <https://bitbucket.org/agronholm/hoi/src/master/examples/schedulers/background.py>`_).
