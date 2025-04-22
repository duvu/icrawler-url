import queue
import time
import json
import re
import mimetypes
from io import BytesIO
from threading import current_thread
from urllib.parse import urlparse

from PIL import Image

from .utils import ThreadPool


class Downloader(ThreadPool):
    """Base class for downloader.

    A thread pool of downloader threads, in charge of downloading files and
    managing image URLs.

    Attributes:
        task_queue (CachedQueue): A queue storing image tasks from the parser.
        signal (Signal): A Signal object shared by all components.
        session (Session): A session object.
        logger: A logging.Logger object used for logging.
        thread_num (int): The number of downloader threads.
        lock (Lock): A threading.Lock object.
    """

    def __init__(self, thread_num=1, signal=None, session=None, storage=None):
        """Initialize with thread pool settings."""
        super().__init__(thread_num, out_queue=None, name="downloader")
        self.signal = signal
        self.session = session
        self.file_idx_offset = 0
        self.clear_status()

    def clear_status(self):
        """Reset fetched_num to 0."""
        self.fetched_num = 0

    def set_file_idx_offset(self, file_idx_offset=0):
        """Set offset of file index."""
        if isinstance(file_idx_offset, int):
            self.file_idx_offset = file_idx_offset
        else:
            raise ValueError('"file_idx_offset" must be an integer')

    def get_filename(self, task, default_ext):
        """Generate a filename based on the URL."""
        url_path = urlparse(task["file_url"])[2]
        extension = url_path.split(".")[-1] if "." in url_path else default_ext
        file_idx = self.fetched_num + self.file_idx_offset
        return f"{file_idx:06d}.{extension}"

    def reach_max_num(self):
        """Check if reached max num."""
        if self.signal.get("reach_max_num"):
            return True
        if self.max_num > 0 and self.fetched_num >= self.max_num:
            return True
        return False

    def keep_file(self, task, response, **kwargs):
        """Determine if a file should be kept. Override in subclasses."""
        return True

    def download(self, task, default_ext, timeout=5, max_retry=3, **kwargs):
        """Process a URL task.
        
        Args:
            task (dict): The task dict with the file_url.
            timeout (int): Timeout for requests.
            max_retry (int): Max retry attempts.
            **kwargs: Additional arguments for subclasses.
        """
        file_url = task["file_url"]
        task["success"] = False
        task["filename"] = None
        retry = max_retry

        while retry > 0 and not self.signal.get("reach_max_num"):
            try:
                response = self.session.get(file_url, timeout=timeout)
            except Exception as e:
                self.logger.error(
                    "Exception caught when downloading file %s, error: %s, remaining retry times: %d",
                    file_url, e, retry - 1
                )
            else:
                if self.reach_max_num():
                    self.signal.set(reach_max_num=True)
                    break
                elif response.status_code != 200:
                    self.logger.error("Response status code %d, file %s", response.status_code, file_url)
                    break
                elif not self.keep_file(task, response, **kwargs):
                    break
                
                with self.lock:
                    self.fetched_num += 1
                    filename = self.get_filename(task, default_ext)
                self.logger.info("image #%s\t%s %s", self.fetched_num, filename, file_url)

                # Mark task as processed
                task["success"] = True
                task["filename"] = filename
                break
            finally:
                retry -= 1

    def start(self, file_idx_offset=0, *args, **kwargs):
        """Start the downloader threads."""
        self.clear_status()
        self.set_file_idx_offset(file_idx_offset)
        self.init_workers(*args, **kwargs)
        for worker in self.workers:
            worker.start()
            self.logger.debug("thread %s started", worker.name)

    def worker_exec(self, max_num, default_ext="", queue_timeout=5, req_timeout=5, max_idle_time=None, **kwargs):
        """Worker thread execution function."""
        self.max_num = max_num
        last_download_time = time.time()

        while True:
            if self.signal.get("reach_max_num"):
                self.logger.info("downloaded images reach max num, thread %s is ready to exit", current_thread().name)
                break

            current_time = time.time()
            if max_idle_time is not None and current_time - last_download_time > max_idle_time and self.fetched_num > 0:
                self.logger.info("no new images for %d seconds, thread %s exit", max_idle_time, current_thread().name)
                break

            try:
                task = self.in_queue.get(timeout=queue_timeout)
            except queue.Empty:
                if self.signal.get("parser_exited"):
                    self.logger.info("no more download task for thread %s", current_thread().name)
                    break
                elif self.fetched_num == 0:
                    self.logger.info("%s is waiting for new download tasks", current_thread().name)
                else:
                    self.logger.info("no more images available, thread %s exit", current_thread().name)
                    break
            except:
                self.logger.error("exception in thread %s", current_thread().name)
            else:
                self.download(task, default_ext, req_timeout, **kwargs)
                self.in_queue.task_done()

        self.logger.info("thread %s exit", current_thread().name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("all downloader threads exited")


class URLCollector(Downloader):
    """Specialized downloader that collects valid image URLs.
    
    Instead of downloading images, this class validates URLs and saves
    them to a JSON file.
    """
    
    def __init__(self, thread_num=1, signal=None, session=None, storage=None, output_file='image_urls.json'):
        """Initialize URL collector.
        
        Args:
            thread_num: Number of threads
            signal: Signal object for communication
            session: Session object for requests
            storage: Not used
            output_file: Path to save the collected URLs
        """
        super().__init__(thread_num, signal, session)
        self.output_file = output_file
        self.urls = []
        
    def is_valid_image_url(self, url, timeout=5):
        """Check if URL points to a valid, accessible image.
        
        Args:
            url: URL to check
            timeout: Request timeout in seconds
            
        Returns:
            bool: True if valid image URL
        """
        # Basic URL format check
        if not re.match(r'^https?://', url):
            self.logger.info(f"Invalid URL format: {url}")
            return False
            
        # Skip non-image URLs
        if 'google.com/search/about-this-image' in url:
            self.logger.info(f"Skipping Google about-this-image URL: {url}")
            return False
            
        if 'wikimedia.org/wiki/' in url or 'wikipedia.org/wiki/' in url:
            self.logger.info(f"Skipping wiki file page: {url}")
            return False
        
        # Check extension for quick validation
        ext = url.split('.')[-1].lower() if '.' in url.split('/')[-1] else None
        if ext and ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp']:
            content_type = mimetypes.guess_type(url)[0]
            if content_type and content_type.startswith('image/'):
                try:
                    response = self.session.head(url, timeout=timeout)
                    if response.status_code == 200:
                        return True
                except Exception as e:
                    self.logger.error(f"Error checking URL {url}: {e}")
                    return False
        
        # Full content validation if needed
        try:
            response = self.session.get(url, timeout=timeout)
            if response.status_code != 200:
                self.logger.info(f"URL returned status code {response.status_code}: {url}")
                return False
                
            content_type = response.headers.get('content-type', '')
            if not content_type.startswith('image/'):
                self.logger.info(f"URL is not an image (content-type: {content_type}): {url}")
                return False
                
            # Try to open as image
            try:
                Image.open(BytesIO(response.content))
                return True
            except Exception as e:
                self.logger.info(f"URL content is not a valid image: {url}, error: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error downloading URL {url}: {e}")
            return False
        
    def download(self, task, default_ext, timeout=5, max_retry=3, **kwargs):
        """Validate and collect image URL.
        
        Args:
            task: Task with file_url
            default_ext: Not used
            timeout: Request timeout
            max_retry: Retry attempts for validation
            **kwargs: Not used
        """
        file_url = task["file_url"]
        
        # Validate URL
        retry = max_retry
        is_valid = False
        
        while retry > 0 and not is_valid:
            is_valid = self.is_valid_image_url(file_url, timeout)
            retry -= 1
            
        if not is_valid:
            self.logger.info(f"Skipping invalid image URL: {file_url}")
            task["success"] = False
            return
        
        # Collect valid URL
        with self.lock:
            self.fetched_num += 1
            self.urls.append(file_url)
            self.logger.info("Valid image URL #%s\t%s", self.fetched_num, file_url)
            
            # Save periodically
            if self.fetched_num % 10 == 0:
                self._save_urls()
                
            if self.reach_max_num():
                self.signal.set(reach_max_num=True)
        
        task["success"] = True
    
    def _save_urls(self):
        """Save URLs to JSON file."""
        try:
            with open(self.output_file, 'w') as f:
                json.dump(self.urls, f, indent=2)
            self.logger.info(f"Saved {len(self.urls)} URLs to {self.output_file}")
        except Exception as e:
            self.logger.error(f"Error saving URLs to file: {e}")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Save all URLs before exiting."""
        self._save_urls()
        self.logger.info(f"All downloader threads exited. Total URLs collected: {len(self.urls)}")
