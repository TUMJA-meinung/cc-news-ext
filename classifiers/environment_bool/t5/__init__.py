import os
from ... import Classifier

from torch import cuda
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from str2bool import str2bool

TASK = "Are you absolutely sure that the following is about environmental hazards?"


class T5(Classifier):
	CACHE = os.path.dirname(__file__) + os.sep + 'cache'

	def __init__(self, base = "mrm8488/t5-base-finetuned-boolq"):
		"""This generates the tokenizer and model for classifying the crawls"""
		if not cuda.is_available():
			print("Warning: No GPU available")
		self.tokenizer = AutoTokenizer.from_pretrained(base, device_map="auto")
		# load a trained version from cache, else regenerate
		try:
			model = AutoModelForSeq2SeqLM.from_pretrained(self.CACHE, local_files_only=True, device_map="auto")
		except:
			model = self.create_model(base)
		# turn off training mode
		model.eval()
		self.model = model

	def create_model(self, base):
		model = AutoModelForSeq2SeqLM.from_pretrained(base, device_map="auto")
		# OPTIONAL: finetune model to respective task
		model.save_pretrained(self.CACHE)
		return model

	def classify(self, context, question=TASK):
		return str2bool(
			self.tokenizer.decode(
				token_ids=self.model.generate(
					input_ids=self.tokenizer.encode(
						'question:'+question+'\ncontext:'+context,
						return_tensors='pt'
					),
					max_new_tokens=2
				)[0],
				skip_special_tokens=True
			)
		)
