from __future__ import annotations

import logging
import random
import time
from collections.abc import Mapping
from urllib.parse import urlsplit

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from .. import defaults
from .proxy_pool import ProxyPool

# Add import for decompression libraries
try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False
    

class Session(requests.Session):
    def __init__(
        self, proxy_pool: ProxyPool | None = None, headers: Mapping | None = None, cookies: Mapping | None = None
    ):
        super().__init__()
        self.logger = logging.getLogger("cscholars.connection")
        self.proxy_pool = proxy_pool
        
        # Default headers that mimic a regular browser
        default_headers = {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        
        self.headers.update(default_headers)
        
        if headers is not None:
            self.headers.update(headers)
        if cookies is not None:
            self.cookies.update(cookies)
        
        # Store previous requests to establish referer relationships
        self.last_request_url = None

    def _get_random_user_agent(self):
        """Return a random, realistic user agent string"""
        user_agents = [
            # Chrome on Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            # Firefox on Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
            # Chrome on macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            # Safari on macOS
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            # Chrome on Linux
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        ]
        return random.choice(user_agents)

    def _url_scheme(self, url):
        return urlsplit(url).scheme

    def _add_human_behavior(self):
        """Simulate human browsing behavior with random delays"""
        # Random delay between 1 and 5 seconds
        time.sleep(random.uniform(1, 5))

    def _decompress_content(self, response):
        """Decompress content based on content-encoding header"""
        content = response.content
        content_encoding = response.headers.get('content-encoding', '').lower()
        
        self.logger.debug(f"Content-Encoding: {content_encoding}")
        
        # Handle Brotli compressed content
        if BROTLI_AVAILABLE and (content_encoding == 'br' or content[:4] == b'\x1b\x92\r\n'):
            try:
                self.logger.debug("Attempting Brotli decompression")
                content = brotli.decompress(content)
                self.logger.debug("Brotli decompression successful")
            except Exception as e:
                self.logger.debug(f"Brotli decompression failed: {e}")
        
        # The regular requests library already handles gzip and deflate automatically
        # but we'll keep this logic for completeness and future extension
        
        return content
    
    @retry(
        stop=stop_after_attempt(1),  # Changed from defaults.MAX_RETRIES to 1
        wait=wait_random_exponential(exp_base=defaults.BACKOFF_BASE),
        retry=retry_if_exception_type((requests.RequestException, requests.HTTPError, requests.ConnectionError)),
    )
    def request(self, method, url, *args, **kwargs):
        # Add human-like behavior
        self._add_human_behavior()
        
        # Set referer from previous request if not already specified
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        
        if 'Referer' not in kwargs['headers'] and self.last_request_url is not None:
            kwargs['headers']['Referer'] = self.last_request_url
        
        # Occasionally rotate user agent
        if random.random() < 0.2:  # 20% chance to change user agent
            self.headers['User-Agent'] = self._get_random_user_agent()

        message = f"{method}ing {url}"
        if args and kwargs:
            message += f" with {args} and {kwargs}"
        elif args:
            message += f" with {args}"
        elif kwargs:
            message += f" with {kwargs}"
        self.logger.debug(message)

        if self.proxy_pool is not None:
            proxy = self.proxy_pool.get_next(protocol=self._url_scheme(url))
            if proxy is not None:  # Check if a proxy was actually returned
                self.logger.debug(f"Using proxy: {proxy.format()}")
                try:
                    response = super().request(method, url, *args, proxies=proxy.format(), **kwargs)
                    response.raise_for_status()
                    self.proxy_pool.increase_weight(proxy)
                except (requests.ConnectionError, requests.HTTPError):
                    self.proxy_pool.decrease_weight(proxy)
                    raise
            else:  # No suitable proxy found, make request without proxy
                self.logger.debug("No suitable proxy found, making request without proxy.")
                response = super().request(method, url, *args, **kwargs)
        else:
            response = super().request(method, url, *args, **kwargs)

        # Only attempt to decompress content for non-binary responses
        if 'image' not in response.headers.get('content-type', ''):
            # Store original content
            original_content = response.content
            # Try to decompress
            decompressed_content = self._decompress_content(response)
            
            # Only replace if decompression actually did something
            if decompressed_content != original_content:
                # Monkey patch the content property
                response._content = decompressed_content

        # Store this URL for potential future referer
        self.last_request_url = url
        
        if "set-cookie" in response.headers:
            self.cookies.update(response.cookies)
        return response
