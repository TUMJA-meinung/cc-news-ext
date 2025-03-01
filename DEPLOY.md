If no Python 3.12 available, insert into `iterators/cc_news.py`:
```py
# === POLYFILLS ===
import sys
if sys.version_info.major <= 3 and sys.version_info.minor <= 11:
	def batched(iterable, n):
		if n < 1:
			raise ValueError('n must be >= 1')
		it = iter(iterable)
		while (batch := list(itertools.islice(it, n))):
			yield batch
	itertools.batched = batched
```

# Servers
## login.bgd.ed.tum.de
```sh
DIR=meinung				# your working directory
mkdir /glob/g01-cache/bgdm/$DIR		# NAS shared with DGX, bgdm allows access
cd /glob/g01-cache/bgdm/$DIR
srun hostname				# should be tulrbgd-g01.bgd.ed.tum.de (DG
sbatch --gpus=1 BATCH.sh		# returns job ID with slurm-[ID].out
squeue					# show current queue
srun --jobid=$ID nvidia-smi		# show current GPU utilization
sacct -j $ID --format=JobID,JobName,State,Elapsed	# show elapsed time
scancel $ID				# cancel job with $ID
sacct					# show current jobs
```

## lxhalle.in.tum.de (incompatible)
```sh
cd cc-news-ext
python3 -m venv ../cc-news-ext_venv
tmux
ulimit -l 10000000 -v 10000000
source ../cc-news-ext_venv/bin/activate
python3 -m pip install -r requirements.txt
nice ./generate.py --urls urls_meinung.csv dataset.csv
```
