# coding: utf-8
from __future__ import absolute_import
from threading import Thread, Event

from windmill.schedulers.base import BaseScheduler
from windmill.schedulers.blocking import BlockingScheduler
from windmill.util import asbool


class BackgroundScheduler(BlockingScheduler):
    """
    A scheduler that runs in the background using a separate thread
    (:meth:`~windmill.schedulers.base.BaseScheduler.start` will return immediately).

    Extra options:

    ========== ============================================================================================
    ``daemon`` Set the ``daemon`` option in the background thread (defaults to ``True``,
               see `the documentation <https://docs.python.org/3.4/library/threading.html#thread-objects>`_
               for further details)
    ========== ============================================================================================
    """

    _thread = None

    def _configure(self, conf):
        self._daemon = asbool(conf.pop('daemon', True))
        super(BackgroundScheduler, self)._configure(conf)

    def start(self):
        BaseScheduler.start(self)
        self._event = Event()
        self._thread = Thread(target=self._main_loop, name='windmill')
        self._thread.daemon = self._daemon
        self._thread.start()

    def shutdown(self, wait=True):
        super(BackgroundScheduler, self).shutdown(wait)
        self._thread.join()
        del self._thread
