# Extended CC-News Dataset

## Install
```sh
python3 -m ensurepip
TMPDIR=/var/tmp python3 -m pip install -r requirements.txt
chmod +x generate.py
```

## Execute
### Preparation
To use the same hostnames as in CC-News, download `https://huggingface.co/datasets/vblagoje/cc_news?sql_console=true&sql=SELECT+DISTINCT+domain+FROM+train%3B` into `query_result.parquet`.
```sh
python3 -c 'import pandas as pd; pd.read_parquet("query_result.parquet").to_csv("urls.txt", index=False, header=False)'
sed 's#^www\.##g' -i urls.txt
```

### Generation
Recommendation: 8GB RAM, 1-4 GPUs (depending on used classifier)
```sh
./generate.py --urls urls.txt dataset.csv
```
For further information see `python3 generate.py -h`.

## Reliability
Depending on the inter-coder reliability, you may have to finetune the classifier.

## Sources
- Common Crawl: <https://commoncrawl.org/get-started#file-cc_fetch_page-py-L32>
- Datadiligence: <https://github.com/Spawning-Inc/datadiligence.git>
- CCNews: methodology based on <https://huggingface.co/datasets/vblagoje/cc_news> (based on <https://github.com/fhamborg/news-please>), then improved and recreated

*For further sources see classifiers.*