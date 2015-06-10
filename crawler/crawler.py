#coding: utf-8
from bs4 import BeautifulSoup

from threading import Thread, RLock
from queue import Queue, PriorityQueue
import datetime, time

import urllib.parse, urllib.request, urllib.error
import os.path

from logging import Logger,getLogger,basicConfig,StreamHandler,DEBUG,INFO


def create_logger(name,level=DEBUG):
    logger = getLogger(name)
    handler = StreamHandler()
    logger.setLevel(INFO)
    handler.setLevel(INFO)
    logger.addHandler(handler)
    return logger

logger = create_logger(__name__)

class ValueObject(object): pass

class Link(ValueObject):
    def __init__(self,anchor, origin=None):
        origin_url = ''
        if origin:
            origin_url = origin.url()

        origin_url = urllib.parse.quote( origin_url, safe=";+:*/?_]}[{\|^~-=!\"#$%&'()")
        origin_pr = urllib.parse.urlparse( origin_url )

        anchor = urllib.parse.quote( anchor , safe=";+:*/?_]}[{\|^~-=!\"#$%&'()")
        anchor_pr = urllib.parse.urlparse( anchor )

        _raw_url = urllib.parse.ParseResult(
                    scheme   = anchor_pr.scheme   or origin_pr.scheme,
                    netloc   = anchor_pr.netloc   or origin_pr.netloc,
                    path     = anchor_pr.path     or origin_pr.path,
                    params   = anchor_pr.params   or origin_pr.params,
                    query    = anchor_pr.query    or origin_pr.query,
                    fragment = anchor_pr.fragment or origin_pr.fragment,
                   ).geturl()

        self._raw_url = _raw_url

        assert(urllib.parse.urlparse(self._raw_url).netloc != '')

    def __hash__(self):
        return self.url().__hash__()
    def __lt__(self,other):
        return self.url().__lt__(other.url())

    def url(self):
        pr = urllib.parse.urlparse( self._raw_url )
        return urllib.parse.ParseResult(
                scheme = pr.scheme,
                netloc = pr.netloc,
                path = pr.path.strip(),
                params = "",
                query = pr.query,
                fragment = ""
                ).geturl()

    def domain(self):
        return urllib.parse.urlparse(self.url()).netloc

    def parse_result(self):
        return urllib.parse.urlparse(self.url())

class Webpage(ValueObject):
    def __init__(self,link,html,soup=None):
        self.link = link

        self.html = html
        if soup is None:
            self.soup = BeautifulSoup(html)

    def links(self):
        anchors = filter(self._valid_anchor, ( link.get("href","") for link in self.soup.find_all('a') ) )
        return ( Link(anchor, origin = self.link ) for anchor in anchors )

    def internal_links(self):
        mydomain = self.link.domain()
        return filter( lambda link: link.domain() == mydomain, self.links())

    def external_links(self):
        mydomain = self.link.domain()
        return filter( lambda link: link.domain() != mydomain, self.links())

    def title(self):
        return self.soup.title.text

    def _valid_anchor(self, anchor):
        path = urllib.parse.urlparse(anchor).path
        if any( path.endswith(s) for s in [".zip",".rar",".tar",".gz",".lzh",".jpg",".jpeg",".png",".gif"] ):
            return False
        if any( (ngword in anchor.lower()) for ngword in ["login","register","signup","signin"] ):
            return False
        return True

class CrawlLinkSelectService(object):
    import os.path
    def __init__(self):
        pass

    def crawlable_links(self,webpage):
        links = self._external_links(webpage)

        links = filter(self._valid_path, links)
        links = filter(self._crawlable_domain, links)
        links = map(self._project_to_root, links)
        return set(links)

    def _project_to_root(self, link):
        pr = link.parse_result()
        new_url = urllib.parse.ParseResult(
                scheme = pr.scheme,
                netloc = pr.netloc,
                path = "",
                params = "",
                query = "",
                fragment = ""
                ).geturl()
        return Link(new_url)

    def _external_links(self,webpage):
        mydomain = webpage.link.domain()
        return filter( lambda link: link.domain() != mydomain, webpage.links())

    def _valid_path(self,link):
        basename,ext = os.path.splitext( link.parse_result().path )

        if not (ext is None or ext == '' or ext in ["html","htm","xml"]):
            return False

        if any( (ngword in basename) for ngword in ["login","register","signup","signin"] ):
            return False

        return True

    def _crawlable_domain(self,link):
        domain = link.domain()
        segments = domain.split(".")

        if any( (d in segments) for d in ["google","hatena","yahoo","twitter","facebook"] ):
            return False
        return True

class DomainEvent(object):
    def __init__(self):
        self.handlers = []
    
    def add(self, handler):
        self.handlers.append(handler)
        return self
    
    def remove(self, handler):
        self.handlers.remove(handler)
        return self
    
    def notify(self, sender, *args, **kwargs):
        for handler in self.handlers:
            handler(sender, *args, **kwargs)
    
    __iadd__ = add
    __isub__ = remove
    __call__ = notify

class VisitWebsiteEvent(DomainEvent):
    def notify(self, sender, *args, **kwargs):
        #TODO 並列化
        for handler in self.handlers:
            handler(sender, *args, **kwargs)

class VisitWebsiteEventHandler(object):
    def handle(self,sender,webpage):
        pass


class RssDetectingService(VisitWebsiteEventHandler):
    def __init__(self):
        pass

    def handle(self,sender,webpage):
        rss = webpage.soup.find('link', type='application/rss+xml')
        if rss: 
            anchor = rss['href']
            link = Link(anchor, webpage.link )
            print(" # ",link.url())

class CrawlToNextLinkService(VisitWebsiteEventHandler):
    def __init__(self, download_link_queue):
        self.download_link_queue = download_link_queue

        self.crawl_link_select_service = CrawlLinkSelectService()
        self.footprint = set()

    def handle(self,sender,webpage):
        next_links = list(self.crawl_link_select_service.crawlable_links(webpage))
        priority = 1
        for next_link in next_links:
            if next_link.url() not in self.footprint:
                self.footprint.add( next_link.url() )
                self.download_link_queue.put( ( priority, next_link ))



class Downloader(object):
    SLEEP_ON_CALLING_LATER = 0.0
    ACCESS_INTERVAL_SECONDS = 60

    def __init__(self, download_link_queue, workers=4, visit_website_event=VisitWebsiteEvent()):
        self.download_link_queue = download_link_queue
        self.workers = workers

        #TODO: should compared and swapped
        self.domain_last_access = {}

        self.visit_website_event = visit_website_event

        self.lock = RLock()

    def run(self):
        threads = ( Thread(target=self._worker, daemon = False, name = "worker %d" % i ) for i in range(self.workers) )
        for t in threads: 
            t.start()
        for t in threads: 
            t.join()

    def _worker(self):
        while True:
            _,link = self.download_link_queue.get()
            try:
                basename,ext = os.path.splitext( link.parse_result().path )
                if self._declare_to_download(link):
                    logger.info("qsize=%d, downloading %s" % (self.download_link_queue.qsize(), link.url() ) )
                    webpage = self._download(link)
                    if webpage:
                        self.visit_website_event(self,webpage)
                    else:
                        #ダウンロード失敗したらなにもしない
                        pass

            finally:
                self.download_link_queue.task_done()

    def _declare_to_download(self,link):
            now = datetime.datetime.now()

            too_early = False
            with self.lock:
                next_access = self.domain_last_access.get( link.domain() , datetime.datetime.min )\
                                + datetime.timedelta(seconds = self.ACCESS_INTERVAL_SECONDS)
                too_early = now < next_access
                if not too_early:
                    self.domain_last_access[ link.domain() ] = now
                    return True

            if too_early:
                self.download_link_queue.put( (1000, link) )
                logger.debug("too early to download. retry %s" % link.url())
                time.sleep(self.SLEEP_ON_CALLING_LATER)

            return False

    def _download(self, link):
        webpage = None
        try:
            html = self._get_html(link)
            webpage = Webpage(link,html)
        except urllib.error.HTTPError as e:
            logger.debug(e)
        except Exception as e:
            logger.debug(e)
        finally:
            return webpage

    def _get_html(self,link):
        response = urllib.request.urlopen(link.url())
        return response.read()


if __name__=="__main__":
    download_link_queue = PriorityQueue()
    parsing_webpage_queue = Queue()

    seeds = ["http://jp.techcrunch.com/","http://blog.livedoor.com/category/243/"]

    for seed in seeds: 
        download_link_queue.put( (0, Link(seed)) )

    visit_website_event  = VisitWebsiteEvent()
    visit_website_event += RssDetectingService().handle
    visit_website_event += CrawlToNextLinkService( download_link_queue ).handle

    workers = 4
    downloader = Downloader( download_link_queue, workers, visit_website_event )
    downloader.run()

