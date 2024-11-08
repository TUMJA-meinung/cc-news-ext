#!/usr/bin/env python3
import sys
import csv
import argparse
import itertools
from classifiers.environment_bool import DeBERTa as EnvironmentBool
from iterators import CCNews

from datasets import IterableDataset

DESC = "CSV-Generator for Extended CommonCrawl News Dataset"
FOOTER = "© 2024 The Authors"


def main(urls = None, limit = None, file = None, where = None):
	classifier = EnvironmentBool()
	data = IterableDataset.from_generator(CCNews, gen_kwargs={"urls":urls})
	data = data.map(lambda entry: {**entry,
		"category": "environmental hazard" if classifier.classify(entry["maintext"]) else None
	})
	if where is not None:
		data = data.filter(where)
	if type(limit)==int and limit >= 0:
		data = data.take(limit)
	data = iter(data)
	if file is None:
		file = open(sys.stdout.fileno(), 'w', newline='', closefd=False)
	else:
		file = open(file, 'w', newline='')
	first = next(data)
	with file as handle:
		writer = csv.DictWriter(handle, fieldnames=first.keys())
		writer.writeheader()
		for line in itertools.chain([first], data):
			writer.writerow(line)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description = DESC, epilog = FOOTER)
	parser.add_argument("-l", "--limit", type=int, default=None,
		help="limit output to n datasets. If a list of urls is given, the limit applies to each url individually.")
	parser.add_argument("-u", "--urls", default="-", type=argparse.FileType('r'),
		help="file with newline separated urls to filter for ('-' for stdin)")
	parser.add_argument("file", default=None, help="output file (default: stdout)")
	args = parser.parse_args()
	main(
		urls = {url.strip() for url in args.urls.readlines()} if args.urls else None,
		limit = args.limit,
		file = args.file,
		where = lambda entry: entry["language"]=="en" and \
			entry["category"]=="environmental hazard"
	)
