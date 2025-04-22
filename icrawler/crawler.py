"""Crawler base class"""

import logging
import sys
import time

from . import defaults
from .downloader import Downloader, URLCollector
from .feeder import Feeder
from .parser import Parser
from .utils import ProxyPool, Session, Signal


class Crawler:
    """Base class for crawlers

    Attributes:
        session (Session): A Session object.
        feeder (Feeder): A Feeder object.
        parser (Parser): A Parser object.
        downloader (Downloader): A Downloader object.
        signal (Signal): A Signal object shared by all components,
                         used for communication among threads
        logger (Logger): A Logger object used for logging
    """

    def __init__(
        self,
        feeder_cls=Feeder,
        parser_cls=Parser,
        downloader_cls=Downloader,
        feeder_threads=1,
        parser_threads=1,
        downloader_threads=1,
        storage=None,  # Kept for backward compatibility, not used
        log_level=logging.INFO,
        extra_feeder_args=None,
        extra_parser_args=None,
        extra_downloader_args=None,
    ):
        """Init components with class names and other arguments.

        Args:
            feeder_cls: class of feeder
            parser_cls: class of parser
            downloader_cls: class of downloader
            feeder_threads: thread number used by feeder
            parser_threads: thread number used by parser
            downloader_threads: thread number used by downloader
            storage: Deprecated parameter, kept for backward compatibility
            log_level: logging level for the logger
        """

        self.set_logger(log_level)
        self.set_proxy_pool()
        self.set_session()
        self.init_signal()
            
        # set feeder, parser and downloader
        feeder_kwargs = {} if extra_feeder_args is None else extra_feeder_args
        parser_kwargs = {} if extra_parser_args is None else extra_parser_args
        downloader_kwargs = {} if extra_downloader_args is None else extra_downloader_args
        
        self.feeder = feeder_cls(feeder_threads, self.signal, self.session, **feeder_kwargs)
        self.parser = parser_cls(parser_threads, self.signal, self.session, **parser_kwargs)
        self.downloader = downloader_cls(
            downloader_threads, self.signal, self.session, None, **downloader_kwargs
        )
        # connect all components
        self.feeder.connect(self.parser).connect(self.downloader)

    def set_logger(self, log_level=logging.INFO):
        """Configure the logger with log_level."""
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=log_level, stream=sys.stderr
        )
        self.logger = logging.getLogger(__name__)
        logging.getLogger("requests").setLevel(logging.WARNING)

    def init_signal(self):
        """Init signal

        3 signals are added: ``feeder_exited``, ``parser_exited``,
        and ``reach_max_num``.
        """
        self.signal = Signal()
        self.signal.set(feeder_exited=False, parser_exited=False, reach_max_num=False)

    def set_proxy_pool(self, pool=None):
        """Construct a proxy pool

        By default no proxy is used.

        Args:
            pool (ProxyPool, optional): a :obj:`ProxyPool` object
        """
        self.proxy_pool = ProxyPool() if pool is None else pool

    def set_session(self, headers=None):
        """Init session with default or custom headers

        Args:
            headers: A dict of headers (default None, thus using the default
                     header to init the session)
        """
        if headers is None:
            headers = defaults.DEFAULT_HEADERS
        elif not isinstance(headers, dict):
            raise TypeError('"headers" must be a dict object')

        self.session = Session(self.proxy_pool)
        self.session.headers.update(headers)

    def crawl(self, feeder_kwargs=None, parser_kwargs=None, downloader_kwargs=None):
        """Start crawling

        This method will start feeder, parser and download and wait
        until all threads exit.

        Args:
            feeder_kwargs (dict, optional): Arguments to be passed to ``feeder.start()``
            parser_kwargs (dict, optional): Arguments to be passed to ``parser.start()``
            downloader_kwargs (dict, optional): Arguments to be passed to
                ``downloader.start()``
        """
        self.signal.reset()
        self.logger.info("start crawling...")

        feeder_kwargs = {} if feeder_kwargs is None else feeder_kwargs
        parser_kwargs = {} if parser_kwargs is None else parser_kwargs
        downloader_kwargs = {} if downloader_kwargs is None else downloader_kwargs

        self.logger.info("starting %d feeder threads...", self.feeder.thread_num)
        self.feeder.start(**feeder_kwargs)

        self.logger.info("starting %d parser threads...", self.parser.thread_num)
        self.parser.start(**parser_kwargs)

        self.logger.info("starting %d downloader threads...", self.downloader.thread_num)
        self.downloader.start(**downloader_kwargs)

        while True:
            if not self.feeder.is_alive():
                self.signal.set(feeder_exited=True)
            if not self.parser.is_alive():
                self.signal.set(parser_exited=True)
            if not self.downloader.is_alive():
                break
            time.sleep(1)

        if not self.feeder.in_queue.empty():
            self.feeder.clear_buffer()
        if not self.parser.in_queue.empty():
            self.parser.clear_buffer()
        if not self.downloader.in_queue.empty():
            self.downloader.clear_buffer(True)

        self.logger.info("Crawling task done!")
