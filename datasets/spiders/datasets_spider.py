import logging
import re
from typing import Any
from urllib.parse import urlparse
from scrapy.http import Request, XmlResponse, Response
from scrapy.spiders import Spider
from scrapy.utils.gz import gunzip, gzip_magic_number
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots

logger = logging.getLogger(__name__)


class DatasetsSpider(Spider):
    name = 'datasets_spider'
    sitemap_urls = (
        'https://paperswithcode.com/sitemap.xml',
    )
    sitemap_rules = [("", "parse")]
    sitemap_follow = ["datasets"]
    sitemap_alternate_links = False

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._cbs = []
        for r, c in self.sitemap_rules:
            if isinstance(c, str):
                c = getattr(self, c)
            self._cbs.append((regex(r), c))
        self._follow = [regex(x) for x in self.sitemap_follow]

    def start_requests(self):
        for url in self.sitemap_urls:
            yield Request(url, self._parse_sitemap)

    def sitemap_filter(self, entries):
        """This method can be used to filter sitemap entries by their
        attributes, for example, you can filter locs with lastmod greater
        than a given date (see docs).
        """
        for entry in entries:
            yield entry

    def _parse_sitemap(self, response):
        if response.url.endswith("/robots.txt"):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield Request(url, callback=self._parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                logger.warning(
                    "Ignoring invalid sitemap: %(response)s",
                    {"response": response},
                    extra={"spider": self},
                )
                return

            s = Sitemap(body)
            it = self.sitemap_filter(s)

            if s.type == "sitemapindex":
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        r = Request(loc, callback=self._parse_sitemap)
                        yield r
            elif s.type == "urlset":
                for loc in iterloc(it, self.sitemap_alternate_links):
                    for r, c in self._cbs:
                        if r.search(loc):
                            r = Request(loc, callback=c)
                            yield r
                            break

    def _get_sitemap_body(self, response):
        """Return the sitemap body contained in the given response,
        or None if the response is not a sitemap.
        """
        if isinstance(response, XmlResponse):
            return response.body
        if gzip_magic_number(response):
            return gunzip(response.body)
        # actual gzipped sitemap files are decompressed above ;
        # if we are here (response body is not gzipped)
        # and have a response for .xml.gz,
        # it usually means that it was already gunzipped
        # by HttpCompression middleware,
        # the HTTP response being sent with "Content-Encoding: gzip"
        # without actually being a .xml.gz file in the first place,
        # merely XML gzip-compressed on the fly,
        # in other word, here, we have plain XML
        if response.url.endswith(".xml") or response.url.endswith(".xml.gz"):
            return response.body

    def parse(self, response):
        url = response.url
        dataset_name = urlparse(url).path.split('/')[-1]
        description = response.xpath('string(//div[@class="description-content"])').get()
        dataloaders = response.xpath('//ul[@class="dataloader-implementations"]/div[@class="row"]')

        # # debug
        # filename = f'./a.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)

        # Extract tasks
        tasks = list()
        tasks_lines = response.xpath(
            '//div[@class="col-md-12"]/ul[@class="list-unstyled"]/li/a')
        if tasks_lines:
            for task in tasks_lines:
                task = task.xpath(
                    './/span[@class="badge badge-primary"]/span/text()').get()
                tasks.append(task)

        # Extract Similar Datasets
        similar_datasets = list()
        similar_datasets_lines = response.xpath(
            '//div[@class="card-deck card-break"]/div[@class="card"][1]/a')
        if similar_datasets_lines:
            for dataset in similar_datasets_lines:
                dataset = dataset.xpath(
                    './@href').get()
                similar_datasets.append(dataset)

        # Extract Paper Count
        paper_info = response.xpath(
            ".//div[@id='datatable-papers_info']/text()").get()
        # paper_count_match = re.search(r'(\d+(,\d+)*)\s*papers', paper_info)
        # # If a match is found, extract the paper count
        # if paper_count_match:
        #     paper_count = paper_count_match.group(1).replace(',', '')
        # else:
        #     paper_count = None

        dataloaders_list = []

        if dataloaders:
            for dataloader in dataloaders:
                # Extract dataloader name and URL
                dataloader_url = dataloader.xpath(
                    './/div[@class="col-md-7"]/div[@class="datal-impl-cell"]/a[@class="code-table-link"]/@href').get()
                if dataloader_url:
                    data_source = re.sub(r"\s+", "", dataloader.xpath(
                        './/a[@class="code-table-link"]/text()[3]').get())
                    dataloaders_list.append({"data_source": data_source,
                                             "url": dataloader_url})
        yield {
            "dataset_name": dataset_name,
            "url": url,
            "description": description,
            "paper_count": paper_info,
            "dataloaders": dataloaders_list,
            "tasks": tasks,
            "similar_datasets": similar_datasets
        }


def regex(x):
    if isinstance(x, str):
        return re.compile(x)
    return x


def iterloc(it, alt=False):
    for d in it:
        yield d["loc"]

        # Also consider alternate URLs (xhtml:link rel="alternate")
        if alt and "alternate" in d:
            yield from d["alternate"]
