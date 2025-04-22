import errno
import queue
import time
import json
import os
from threading import current_thread
from urllib.parse import urlparse

from PIL import Image

from .utils import ThreadPool


class Downloader(ThreadPool):
    """Base class for downloader.

    A thread pool of downloader threads, in charge of downloading files and
    saving them in the corresponding paths.

    Attributes:
        task_queue (CachedQueue): A queue storing image downloading tasks,
            connecting :class:`Parser` and :class:`Downloader`.
        signal (Signal): A Signal object shared by all components.
        session (Session): A session object.
        logger: A logging.Logger object used for logging.
        workers (list): A list of downloader threads.
        thread_num (int): The number of downloader threads.
        lock (Lock): A threading.Lock object.
    """

    def __init__(self, thread_num=1, signal=None, session=None, storage=None):
        """Init Parser with some shared variables."""
        super().__init__(thread_num, out_queue=None, name="downloader")
        self.signal = signal
        self.session = session
        self.storage = storage
        self.file_idx_offset = 0
        self.clear_status()

    def clear_status(self):
        """Reset fetched_num to 0."""
        self.fetched_num = 0

    def set_file_idx_offset(self, file_idx_offset=0):
        """Set offset of file index.

        Args:
            file_idx_offset: It can be either an integer or 'auto'. If set
                to an integer, the filename will start from
                ``file_idx_offset`` + 1.
        """
        if isinstance(file_idx_offset, int):
            self.file_idx_offset = file_idx_offset
        else:
            raise ValueError('"file_idx_offset" must be an integer')

    def get_filename(self, task, default_ext):
        """Set the path where the image will be saved.

        The default strategy is to use an increasing 6-digit number as
        the filename. You can override this method if you want to set custom
        naming rules. The file extension is kept if it can be obtained from
        the url, otherwise ``default_ext`` is used as extension.

        Args:
            task (dict): The task dict got from ``task_queue``.

        Output:
            Filename with extension.
        """
        url_path = urlparse(task["file_url"])[2]
        extension = url_path.split(".")[-1] if "." in url_path else default_ext
        file_idx = self.fetched_num + self.file_idx_offset
        return f"{file_idx:06d}.{extension}"

    def reach_max_num(self):
        """Check if downloaded images reached max num.

        Returns:
            bool: if downloaded images reached max num.
        """
        if self.signal.get("reach_max_num"):
            return True
        if self.max_num > 0 and self.fetched_num >= self.max_num:
            return True
        else:
            return False

    def keep_file(self, task, response, **kwargs):
        return True

    def download(self, task, default_ext, timeout=5, max_retry=3, overwrite=False, **kwargs):
        """Download the image.

        Args:
            task (dict): The task dict got from ``task_queue``.
            timeout (int): Timeout of making requests for downloading images.
            max_retry (int): the max retry times if the request fails.
            **kwargs: reserved arguments for overriding.
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
                    "Exception caught when downloading file %s, " "error: %s, remaining retry times: %d",
                    file_url,
                    e,
                    retry - 1,
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

                # Mark task as success but don't save anything since we have no storage
                task["success"] = True
                task["filename"] = filename
                break
            finally:
                retry -= 1

    def process_meta(self, task):
        """Process some meta data of the images.

        This method should be overridden by users if wanting to do more things
        other than just downloading the image, such as saving annotations.

        Args:
            task (dict): The task dict got from task_queue. This method will
                make use of fields other than ``file_url`` in the dict.
        """
        pass

    def start(self, file_idx_offset=0, *args, **kwargs):
        self.clear_status()
        self.set_file_idx_offset(file_idx_offset)
        self.init_workers(*args, **kwargs)
        for worker in self.workers:
            worker.start()
            self.logger.debug("thread %s started", worker.name)

    def worker_exec(self, max_num, default_ext="", queue_timeout=5, req_timeout=5, max_idle_time=None, **kwargs):
        """Target method of workers.

        Get task from ``task_queue`` and then download files and process meta
        data. A downloader thread will exit in either of the following cases:

        1. All parser threads have exited and the task_queue is empty.
        2. Downloaded image number has reached required number(max_num).
        3. No new downloads for max_idle_time seconds.

        Args:
            max_num (int): Maximum number of images to download
            queue_timeout (int): Timeout of getting tasks from ``task_queue``.
            req_timeout (int): Timeout of making requests for downloading pages.
            max_idle_time (int): Maximum time (in seconds) to wait without receiving new images
            **kwargs: Arguments passed to the :func:`download` method.
        """
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
                self.process_meta(task)
                self.in_queue.task_done()

        self.logger.info("thread %s exit", current_thread().name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("all downloader threads exited")


class ImageDownloader(Downloader):
    """Downloader specified for images."""

    def _size_lt(self, sz1, sz2):
        return max(sz1) <= max(sz2) and min(sz1) <= min(sz2)

    def _size_gt(self, sz1, sz2):
        return max(sz1) >= max(sz2) and min(sz1) >= min(sz2)

    def keep_file(self, task, response, min_size=None, max_size=None):
        """Decide whether to keep the image

        Compare image size with ``min_size`` and ``max_size`` to decide.

        Args:
            response (Response): response of requests.
            min_size (tuple or None): minimum size of required images.
            max_size (tuple or None): maximum size of required images.
        Returns:
            bool: whether to keep the image.
        """
        try:
            from io import BytesIO
            img = Image.open(BytesIO(response.content))
        except OSError:
            return False
        task["img_size"] = img.size
        if min_size and not self._size_gt(img.size, min_size):
            return False
        if max_size and not self._size_lt(img.size, max_size):
            return False
        return True

    def get_filename(self, task, default_ext):
        url_path = urlparse(task["file_url"])[2]
        if "." in url_path:
            extension = url_path.split(".")[-1]
            if extension.lower() not in ["jpg", "jpeg", "png", "bmp", "tiff", "gif", "ppm", "pgm"]:
                extension = default_ext
        else:
            extension = default_ext
        file_idx = self.fetched_num + self.file_idx_offset
        return f"{file_idx:06d}.{extension}"

    def worker_exec(self, max_num, default_ext="jpg", queue_timeout=5, req_timeout=5, max_idle_time=None, **kwargs):
        super().worker_exec(max_num, default_ext, queue_timeout, req_timeout, max_idle_time, **kwargs)


from io import BytesIO
import mimetypes
import re

class URLCollector(Downloader):
    """Special downloader that only collects URLs without downloading files.
    
    Instead of downloading images, this class just collects valid image URLs and saves
    them to a JSON file. It checks if each URL points to a valid, downloadable image
    before adding it to the collection.
    """
    
    def __init__(self, thread_num=1, signal=None, session=None, storage=None, output_file='image_urls.json'):
        """Initialize URLCollector.
        
        Args:
            thread_num: Number of threads to use
            signal: Signal object for inter-thread communication
            session: Session object for making requests
            storage: Storage object (not used in this class, can be None)
            output_file: Path to the JSON file where URLs will be saved
        """
        # Pass an empty dict as storage if None is provided
        # This avoids errors in the parent class initialization
        storage = storage or {}
        super().__init__(thread_num, signal, session, storage)
        self.output_file = output_file
        self.urls = []
        
    def is_valid_image_url(self, url, timeout=5):
        """Check if the URL is a valid, downloadable image.
        
        Args:
            url: URL to check
            timeout: Request timeout in seconds
            
        Returns:
            bool: True if the URL is a valid image, False otherwise
        """
        # Simple URL format check
        if not re.match(r'^https?://', url):
            self.logger.info(f"Invalid URL format: {url}")
            return False
            
        # Skip Google 'about-this-image' URLs
        if 'google.com/search/about-this-image' in url:
            self.logger.info(f"Skipping Google about-this-image URL: {url}")
            return False
            
        # Skip wiki file pages (not direct image links)
        if 'wikimedia.org/wiki/' in url or 'wikipedia.org/wiki/' in url:
            self.logger.info(f"Skipping wiki file page: {url}")
            return False
        
        # Try to get the content type from the URL extension
        ext = url.split('.')[-1].lower() if '.' in url.split('/')[-1] else None
        if ext and ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp']:
            content_type = mimetypes.guess_type(url)[0]
            if content_type and content_type.startswith('image/'):
                # Still do a HEAD request to verify URL is accessible
                try:
                    response = self.session.head(url, timeout=timeout)
                    if response.status_code == 200:
                        return True
                except Exception as e:
                    self.logger.error(f"Error checking URL {url}: {e}")
                    return False
        
        # If extension doesn't provide enough info, do a GET request and check content
        try:
            response = self.session.get(url, timeout=timeout)
            if response.status_code != 200:
                self.logger.info(f"URL returned status code {response.status_code}: {url}")
                return False
                
            content_type = response.headers.get('content-type', '')
            if not content_type.startswith('image/'):
                self.logger.info(f"URL is not an image (content-type: {content_type}): {url}")
                return False
                
            # Try to open the content as an image
            try:
                Image.open(BytesIO(response.content))
                return True
            except Exception as e:
                self.logger.info(f"URL content is not a valid image: {url}, error: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error downloading URL {url}: {e}")
            return False
        
    def download(self, task, default_ext, timeout=5, max_retry=3, overwrite=False, **kwargs):
        """Instead of downloading, check if URL is valid and collect it.
        
        Args:
            task: Task dictionary containing file_url
            default_ext: Not used
            timeout: Request timeout in seconds
            max_retry: Number of retries if validation fails
            overwrite: Not used
            **kwargs: Additional arguments
        """
        file_url = task["file_url"]
        
        # Verify that the URL is a valid image
        retry = max_retry
        is_valid = False
        
        while retry > 0 and not is_valid:
            is_valid = self.is_valid_image_url(file_url, timeout)
            retry -= 1
            
        if not is_valid:
            self.logger.info(f"Skipping invalid image URL: {file_url}")
            task["success"] = False
            task["filename"] = None
            return
        
        with self.lock:
            self.fetched_num += 1
            self.urls.append(file_url)
            self.logger.info("Valid image URL #%s\t%s", self.fetched_num, file_url)
            
            # Save to JSON file periodically
            if self.fetched_num % 10 == 0:
                self._save_urls()
                
            if self.reach_max_num():
                self.signal.set(reach_max_num=True)
        
        # Mark task as success
        task["success"] = True
        task["filename"] = None
    
    def _save_urls(self):
        """Save collected URLs to a JSON file."""
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
