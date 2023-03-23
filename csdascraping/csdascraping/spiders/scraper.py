import html as h
import json
import re
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser

import lxml.html
import requests
import scrapy
from lxml import html as HTML


from scrapy import Request, Spider
from scrapy_splash import SplashRequest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urlparse
import boto3
from boto3.dynamodb.conditions import Key
from scrapy.selector import Selector
from botocore.exceptions import ClientError
from scrapy.http import HtmlResponse
from html import unescape
import hashlib
from boto3.dynamodb.conditions import Key
from csdascraping import settings
import tldextract
from urllib.parse import urlsplit
from urllib.parse import urlparse, urlunsplit, urljoin
from collections import OrderedDict
import time


class URLSpider(scrapy.Spider):
    name = "urls"
    start_urls = ["https://www.earthdata.nasa.gov/esds/csda"]    
    excluded_words = [
        "accounts",
        "github",
        "youtube",
        "apple",
        "twitter",
        "atlassian",
        "irs",
        "usa",
        "developer",
        "sports"
        # "api",
        # "contact",
        # "login",
        # "twitter",
        # "youtube",
        # "register",
        # "citations",
        # "terms",
        # "privacy",
        # "community",
        # "support",
        # "feedback",
        # "logout",
        # "github",
        # "issues",
        # "release",
        # "copyrights",
        # "doi",
        # "publications",
        # "archieve",
        # "brands",
        # "docs",
        # "keywords",
        # "search",
        # "centers",
        # "media",
        # "programs",
        # "maps",
        # "media",
        # "about",
        # "news",
        # "community",
        # # "data",
        # "jobs",
    ]

    def __init__(self, *args, **kwargs):
        super(URLSpider, self).__init__(*args, **kwargs)
        self.visited_urls = set()
        self.urls = self.start_urls
        self.excluded_words= self.__class__.excluded_words
        self.base_url = None
        self.max_depth = settings.MAX_DEPTH
        # self.driver = self.create_driver()
        self.results = []
        self.visited_inner_urls = {}
        self.visited_inner_urls_list = []
        self.dynamodb = boto3.resource("dynamodb")
        self.table_name = settings.DYNAMODB_TABLE_NAME
        self.table = self.dynamodb.Table(self.table_name)
        self.page_count = {}

        # function restricts crawler to scrape only the allowed domains and returns the list of domains
    def get_allowed_domains(self):
        domains = []

        for url in self.start_urls:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            if domain.startswith("www."):
                domain = domain[4:]
            if not any(word in domain for word in self.excluded_words):
                domains.append(domain)
        # print("allowed_domains", domains)
        return domains

    # function is called when spider is initialized, returns an iterable of requests which spider will use to crawl.
    def start_requests(self):
        allowed_domains = self.get_allowed_domains()
        for url in self.start_urls:
            # print("url", url)
            # print("allowed_domains", allowed_domains)
            yield scrapy.Request(
                url,
                self.parse,
                meta={"depth": 0, "count": 0, "allowed_domains": allowed_domains},
            )

    # function is responsible for extracting URL, content-type and paragraphs from HTML response and returns it as a dictionary
    def parse(self, response, depth=0, count=0):
        if depth >= 3 or response.meta.get('depth', 0) >= 3:
            return

        if response.url in self.visited_urls:
            return
            # Extracting domain name from the response URL

        self.visited_urls.add(response.url)
  

        content_type = response.headers.get("Content-Type", b"").split(b";")[0].decode()
        if content_type not in ["text/html", "application/xhtml+xml"]:
            return

        html = lxml.html.fromstring(response.text)
  

        # Parsing HTML content
        tree = HTML.fromstring(response.text)
        # Checking if INCLUDE_HEADER_FOOTER setting is set to True
        include_header_footer = self.settings.getbool(
            "INCLUDE_HEADER_FOOTER", default=True
        )

        if include_header_footer:
            # Extract all text from HTML content including header and footer
            all_text = " ".join(
                tree.xpath("//text()[not(ancestor::script)][not(ancestor::style)]")
            )
        else:
            # Extract all text from HTML content excluding header and footer
            all_text = " ".join(
                tree.xpath(
                    "//text()[not(ancestor::script)][not(ancestor::style)][not(ancestor::header) and not(ancestor::nav)][not(ancestor::footer)][not(ancestor::img)]"
                )
            )

        stay_within_url = self.settings.getbool("STAY_WITHIN_URL")
        self.base_url = response.url.split("://")[-1].split("/")[0]
        # print("baseurl1",self.base_url )
        # Defining an XPath selector that includes only links within the current domain
        if stay_within_url:
            xpath = f"//a[starts-with(ancestor-or-self::a/@href, '{self.base_url}')]"
        else:
            xpath = "//a"

        for link in tree.xpath(xpath):
            # print("link is ", link)
            href = link.get("href")
            # print("href", href)
            if href:
                # Follow the link if it's within the current domain
                if stay_within_url and not href.startswith(("http", "https", "//")):
                    href = f"{self.base_url}/{href}"
                    # print("href", href)
                if self.base_url in href:
                    yield response.follow(href, callback=self.parse)

        # Cleaning the text
        all_text = re.sub(r"\s+", " ", all_text).strip()

        # Spliting text into paragraphs
        paragraphs = re.split(r"\n{2,}", all_text)

        # Removing empty paragraphs
        paragraphs = [p for p in paragraphs if p.strip()]


        result = {
        "URL": response.url,
        "Content Type": content_type,
        "Paragraphs": paragraphs,
        }
        yield result
        # print("paragraphs", paragraphs)
        if self.base_url is None:
            self.base_url = "/".join(response.url.split("/")[:3])
        # print("base url2", self.base_url)
        # Extracting domain name from the URL
        extracted = tldextract.extract(response.url)
        # print("extracted", extracted)
        if self.base_url.startswith("www."):
            self.base_url = self.base_url[4:]
        # base_domain = f"{extracted.domain}.{extracted.suffix}"
        base_domain = "'" + self.base_url + "'"
        # if base_domain.startswith("www."):
        #     base_domain = base_domain[4:]
        # print("base_domain",base_domain)
        li = self.get_allowed_domains()
        # print("li", li)
        # matching_domains = [x for x in li if x == 'www.' + base_domain.replace('www.', '')]
        #matching_domains = [x for x in li if  base_domain in x]
        print("resall",self.get_allowed_domains() )
        # print(matching_domains)
        # Checking if domain is allowed or not
        if base_domain == self.get_allowed_domains()[0]:
            print("not allowed")
            return
        else:
            print("base domain is allowed to scrap")

        # Checking if any excluded word is present in URL
        if any(word in response.url for word in self.excluded_words):
            return

        # Visiting inner urls
        inner_urls = response.css("a::attr(href)").getall()
        # print("inner urls of url",inner_urls )

                
        for link in inner_urls:
            if not link in self.excluded_words:
                # Extracting domain name from inner urls
                inner_extracted = tldextract.extract(link)
                inner_base_domain = (
                    f"https://{inner_extracted.domain}.{inner_extracted.suffix}"
                )

            if link in self.visited_urls:
                continue
            # self.visited_urls.add(link)
            yield response.follow(link, self.parse, meta={"depth": depth + 1})
      

        for link in response.css("a"):
            # print("responseurl")

            # print(response.url)
            link_url = link.css("::attr(href)").get()
            if link_url is None:
                continue

            # Constructing absolute URL
            link_url = urljoin(response.url, link_url)
            # print("abs", link_url)


            for link in response.css('a::attr(href)').getall():
                absolute_url = response.urljoin(link)
                link_domain = urlparse(absolute_url).hostname
                response_domain = urlparse(response.url).hostname
                if link_domain == response_domain and link != response.url and link_domain in self.get_allowed_domains():
                    yield scrapy.Request(absolute_url, callback=self.parse, meta={'depth': current_depth + 1})


        self.results.append(result)



        # dynamodb = boto3.client("dynamodb")
        # try:
        #     dynamodb.describe_table(TableName=self.table_name)

        # except dynamodb.exceptions.ResourceNotFoundException:
        #     # Table does not exist, so create it
        #     dynamodb.create_table(
        #         TableName=self.table_name,
        #         KeySchema=[
        #             {"AttributeName": "url", "KeyType": "HASH"},
        #         ],
        #         AttributeDefinitions=[
        #             {"AttributeName": "url", "AttributeType": "S"},
        #         ],
        #         ProvisionedThroughput={
        #             "ReadCapacityUnits": 5,
        #             "WriteCapacityUnits": 5,
        #         },
        #     )

        # # Creating an item to insert into the DynamoDB table
        # item = {
        #     "url": {"S": response.url},
        #     "content type": {"S": content_type},
        #     "paragraphs": {"SS": paragraphs},
        # }
        # # Inserting the item into the DynamoDB table
        # dynamodb.put_item(TableName=self.table_name, Item=item)
