import json
import fnmatch
import itertools
from datetime import datetime
from urllib.parse import quote as urlencode
from urllib.parse import urlparse

import urllib3
from urllib3.exceptions import HTTPError
import newsplease
from newsplease import NewsPlease
import datadiligence as dd # auto-checks if API key available
from warcio.archiveiterator import ArchiveIterator
from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0
pool = urllib3.PoolManager(retries=urllib3.util.Retry(
	# commoncrawl.org/blog/oct-nov-2023-performance-issues
	# backoff_max = 30min
	total=20, backoff_factor=1, backoff_max=180, status_forcelist=[429,500,502,503,504]
))


def CCNews(urls = None, balance = "even", batch_size = 10, log = None):
	"""
	urls = supports glob patterns for URL paths
		e.g. nytimes.com/[0-9][0-9][0-9][0-9]/[0-9][0-9]/*
	batch_size = batch size per URL and year (i.e. URL-year), after which the
		next URL-year is processed. After processing all i-th batches of
		each URL-year, the i+1-th batches are fetched. This guarantees a
		balanced representation of URLs and years when fetching only the
		first x items.
	log = writing stream handle, default sys.stdout
	"""
	# get index URLs
	try:req = _request("https://index.commoncrawl.org/collinfo.json")
	except HTTPError as e:
		print("Failed to fetch indices", e, file=log)
		return
	indices = _sort(req.json()), balance=balance)
	for batch in itertools.count():
		# process per URL and year
		for index,url in itertools.product(indices, urls):
			host = url.split("/",1)[0]
			# fetch records from index
			index_url = "{}?url={}&output=json".format(
				index["cdx-api"],
				urlencode(host)
			)
			print("Processing batch {} of {} for {}".format(batch, index["name"], host), file=log)
			try:req = _request(index_url)
			except HTTPError as e:
				print("  └── {}".format(e), file=log)
				continue
			if req.status >= 300:
				print("  └── HTTP Status {}".format(req.status), file=log)
				continue
			records = [
				json.loads(record)
				for record in _read(req).strip().split("\n")
			]
			# filter for URL path patterns
			if "/" in url:
				records = filter(
					lambda rec: fnmatch.fnmatch(
						urlparse(rec["url"]).path,
						rec["url"].split("/",1)[1]
					),
					records
				)
			# filter for current batch
			records = next(itertools.islice(
				itertools.batched(records, n=batch_size), # batches
				batch # index
			), [])
			# process batch
			for record in records:
				offset, length = int(record['offset']), int(record['length'])
				try:stream = _request(
					"https://data.commoncrawl.org/{}".format(record["filename"]),
					headers={"Range":"bytes={}-{}".format(offset, offset+length+1)}
				)
				except HTTPError as e:
					print("  └── {}".format(e), file=log)
					continue
				for resrc in ArchiveIterator(stream, arc2warc=True):
					if resrc.rec_type != "response": continue
					uri = resrc.rec_headers.get_header("WARC-Target-URI")
					# filter for urls
					if urls is not None and uri is not None and not any(u in uri for u in urls):
						continue
					# respect crawl opt-outs
					if not dd.is_allowed(headers = resrc.http_headers.headers): # dd.is_allowed(url=uri) takes much time
						continue
					try:article = NewsPlease.from_warc(resrc)
					except Exception as e:
						print("  └── {}: {}".format(type(e).__name__, e), file=log)
						continue
					if article.maintext is None:
						print("  └── No/empty main text", file=log)
						continue
					if article.language is None and text != "":
						article.language = detect(text)
					yield vars(article)
				stream.close()


# === HELPER METHODS ===
def _sort(items, balance = "even", by = lambda i: i["from"]):
	years = itertools.groupby(
		items,
		lambda item: datetime.fromisoformat(by(item)).year
	)
	if balance == "even": # 2024a, 2023a, 2022a, 2024b, 2023b, 2024c
		return filter(lambda i: i is not None,
			[
				year
				for sublist in itertools.zip_longest(*[
					list(group) for _,group in years
				])
				for year in sublist
			]
		)
	elif balance == "asc":
		return years
	elif balance == "desc":
		return reversed(years)
	raise ValueError("`balance` must be either 'even', 'asc' or 'desc'")

def _request(url, method='GET', headers=dict()):
	return pool.request(method, url, headers=headers, decode_content=True,
		preload_content=False)

def _read(response):
	try:charset = response.info()['content-type'].split('=')[1]
	except:charset = 'utf-8'
	return response.read().decode(charset)