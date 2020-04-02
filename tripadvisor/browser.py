import os
from pathlib import Path

from selenium.webdriver import Chrome, ChromeOptions


def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


@singleton
class Browser:
    """Class controlling the browser instance."""

    headless = False

    if os.name == 'nt':
        CHR_PATH = Path(r'C:\Users\Roel\PycharmProjects\scrapers\tripadvisor\driver\chromedriver.exe')
    else:
        CHR_PATH = Path.cwd() / 'chromedriver' / 'chromedriver'

    def _init_chrome(self, headless: bool = True) -> Chrome:
        chr_opt = ChromeOptions()
        chr_opt.headless = headless
        chr_opt.add_argument('log-level=2')
        chr_opt.add_argument('--disable-logging')
        chr_opt.add_argument('--disable-remote-fonts')
        chr_opt.add_argument('--incognito')

        if 'posix' in os.name:
            chr_opt.binary_location = 'chrome/chrome'

        chrome = Chrome(executable_path=self.CHR_PATH, options=chr_opt)
        chrome.set_window_size(1920, 1080)
        return chrome

    def __init__(self, headless=True):
        """Initialize browser."""
        self.headless = headless

    def __enter__(self):
        """Initialize driver."""
        self.driver = self._init_chrome(self.headless)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close and quit browser instance."""
        if exc_type is not None:
            exc_tb.print_exception(exc_type, exc_val, exc_tb)
        self.driver.close()
        self.driver.quit()
        print('Browser closed...')

    def restart(self):
        """Restart webdriver."""
        self.driver.close()
        print('Restarting browser...')
        self.__enter__()
        return self
