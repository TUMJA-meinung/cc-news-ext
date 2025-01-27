import re
import json
import itertools
from datetime import datetime
from urllib.parse import quote as urlencode

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
	total=20, backoff_factor=1, backoff_max=180, # backoff_max=30min
	status_forcelist=[429,500,502,503,504]
))


def CCNews(urls, balance = "even", batch_size = 10, log = None, verbose = False, start_batch = 0, start_index = None, start_url = None):
	"""
	urls = supports regular expressions for URL paths
		e.g. nytimes.com/\d{4}/\d\d/.*
	batch_size = batch size per URL and year (i.e. URL-year), after which the
		next URL-year is processed. After processing all i-th batches of
		each URL-year, the i+1-th batches are fetched. This guarantees a
		balanced representation of URLs and years when fetching only the
		first x items.
	log = writing stream handle, default sys.stdout
	verbose = verbose logs
	start_batch = batch to start from (for continuing a crawl with same conditions)
	start_index = index to start from (for continuing a crawl with same conditions)
	start_url = URL to start from (for continuing a crawl with same conditions)
	"""
	# get index URLs
	try:req = _request("https://index.commoncrawl.org/collinfo.json")
	except HTTPError as e:
		print("Failed to fetch indices", e, file=log)
		return
	indices = _sort(req.json(), balance=balance)
	for batch in itertools.count(start = start_batch):
		# process per URL and year
		for index,url in itertools.product(indices, urls):
			if start_index is not None: # start from start_index
				if index == start_index:
					start_index = None
				else:
					continue
			if start_url is not None: # start from start_url
				if url == start_url:
					start_url = None
				else:
					continue
			host = url.split("/",1)[0]
			# for filtering
			filter_path = "/" in url
			if filter_path:
				regex_host = re.escape(host)
				path = url.split("/",1)[1]
			# fetch records from index
			# @src https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference
			index_url = "{}?url={}&matchType=prefix&fl=url,offset,length,filename&output=json".format(
				index["cdx-api"],
				urlencode(host)
			)
			if filter_path:
				index_url += "&filter=~url:.*{}/{}$".format(
					urlencode(regex_host),
					urlencode(path)
				)
			print("Processing batch {} of {} for {}".format(batch, index["name"], host), file=log)
			if verbose:
				print("  └── Index: {}".format(index_url), file=log)
			try:req = _request(index_url)
			except HTTPError as e:
				print("  └── {}".format(e), file=log)
				continue
			if req.status == 404:
				print("  └── No crawls for filter", file=log)
				continue
			if req.status >= 300:
				print("  └── HTTP Status {}".format(req.status), file=log)
				continue
			lines = _read(req).splitlines()
			records = map(
				lambda record: json.loads(record),
				lines
			)
			# filter for URL path patterns
			if filter_path:
				records = filter(
					lambda rec: re.match(
						".*"+regex_host+"/"+path+"$",
						rec["url"],
						re.IGNORECASE
					),
					records
				)
			if verbose:
				records = list(records)
				print("  └── Batch {} of {} records".format(
					batch,
					len(lines)
				), file=log)
			# filter for current batch
			records = next(itertools.islice(
				itertools.batched(records, n=batch_size), # batches
				batch, # index
				batch+1
			), [])
			# process batch
			for record in records:
				if verbose:
					print("  └── Fetching {}".format(record["url"]), file=log)
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
					# logging
					if verbose:
						print("    └── Reading {}".format(uri), file=log)
					# filter for urls
					if uri is not None and not host in uri:
						continue
					# respect crawl opt-outs
					if not dd.is_allowed(headers = resrc.http_headers.headers): # dd.is_allowed(url=uri) takes much time
						continue
					if verbose:
						print("    └── Crawl not disallowed".format(uri), file=log)
					try:
						article = NewsPlease.from_warc(resrc)
						if article.maintext is None:
							print("    └── No/empty main text", file=log)
							continue
						if article.language is None and article.maintext != "":
							article.language = detect(article.maintext)
					except Exception as e:
						print("    └── {}: {}".format(type(e).__name__, e), file=log)
						continue
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
