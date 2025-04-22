import logging
import os.path as osp
from argparse import ArgumentParser

from icrawler.builtin import GoogleImageCrawler
from icrawler.downloader import URLCollector


def test_google():
    print("start testing GoogleImageCrawler with URL collection")
    google_crawler = GoogleImageCrawler(
        downloader_cls=URLCollector,
        downloader_threads=1, 
        storage=None,  # Storage is not needed for URLCollector
        log_level=logging.INFO,
        extra_downloader_args={"output_file": "google_image_urls.json"}
    )
    search_filters = dict(size="large", color="orange", license="commercial,modify", date=(None, (2017, 11, 30)))
    google_crawler.crawl("dog", filters=search_filters, max_num=10)


def main():
    parser = ArgumentParser(description="Test Google crawler with URL collection")
    parser.add_argument(
        "--crawler",
        nargs="+",
        default=["google"],
        help="which crawlers to test",
    )
    args = parser.parse_args()
    for crawler in args.crawler:
        if crawler == "google":
            test_google()
        else:
            print(f"Crawler {crawler} is not supported")
        print("\n")


if __name__ == "__main__":
    main()
