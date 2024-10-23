import json
import itertools
from datetime import datetime
import urllib
from urllib.parse import quote as urlencode

import newsplease
from newsplease import NewsPlease
import datadiligence as dd # auto-checks if API key available
from warcio.archiveiterator import ArchiveIterator
from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0


def CCNews(urls = None, balance = "even", batch_size = 10, log = None):
	"""
	batch_size = batch size per URL and year (i.e. URL-year), after which the
		next URL-year is processed. After processing all i-th batches of
		each URL-year, the i+1-th batches are fetched. This guarantees a
		balanced representation of URLs and years when fetching only the
		first x items.
	log = writing stream handle, default sys.stdout
	"""
	# get index URLs
	req = urllib.request.urlopen("https://index.commoncrawl.org/collinfo.json")
	indices = _sort(json.loads(_read(req)), balance=balance)
	for batch in itertools.count():
		# process per URL and year
		for index,url in itertools.product(indices, urls):
			# fetch records from index
			index_url = "{}?url={}&output=json".format(index["cdx-api"],urlencode(url))
			print("Processing batch {} of {} for {}".format(batch, index["name"], url), file=log)
			try:req = urllib.request.urlopen(index_url)
			except urllib.error.HTTPError as e:
				print("  └── {}".format(e))
				continue
			if req.getcode() >= 300:
				print("  └── HTTP Status {}".format(req.getcode()), file=log)
				continue
			records = [json.loads(record)
				for record in _read(req).strip().split("\n")]
			# filter for current batch
			records = next(itertools.islice(
				itertools.batched(records, n=batch_size), # batches
				batch, None # indices
			))
			# process batch
			for record in records:
				offset, length = int(record['offset']), int(record['length'])
				stream = urllib.request.urlopen(
					urllib.request.Request(
						"https://data.commoncrawl.org/{}".format(record["filename"]),
						headers={"Range":"bytes={}-{}".format(offset, offset+length+1)}
					)
				)
				for resrc in ArchiveIterator(stream):
					if resrc.rec_type != "response": continue
					uri = resrc.rec_headers.get_header("WARC-Target-URI")
					# filter for urls
					if urls is not None and not any(u in uri for u in urls):
						continue
					# respect crawl opt-outs
					if not dd.is_allowed(headers = resrc.http_headers.headers): # dd.is_allowed(url=uri) takes much time
						continue
					try:article = NewsPlease.from_warc(resrc)
					except newsplease.EmptyResponseError:
						print("  └── No HTML (news-please EmptyResponseError)", file=log)
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

def _read(response):
	return response.read().decode(
		response.info().get_content_charset("utf-8")
	)
