import os
from ... import Classifier

from torch import cuda, softmax
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from str2bool import str2bool

TASK = "Are you absolutely sure that the following is about environmental hazards?"


class DeBERTa(Classifier):
	CACHE = os.path.dirname(__file__) + os.sep + 'cache'

	def __init__(self, base = "nfliu/deberta-v3-large_boolq"):
		"""This generates the tokenizer and model for classifying the crawls"""
		if not cuda.is_available():
			print("Warning: No GPU available")
		self.tokenizer = AutoTokenizer.from_pretrained(base)
		# load a trained version from cache, else regenerate
		try:
			model = AutoModelForSequenceClassification.from_pretrained(self.CACHE, local_files_only=True)
		except:
			model = self.create_model(base)
		# turn off training mode
		model.eval()
		self.model = model

	def create_model(self, base):
		model = AutoModelForSequenceClassification.from_pretrained(base)
		# OPTIONAL: finetune model to respective task
		model.save_pretrained(self.CACHE)
		return model

	def classify(self, context, question=TASK):
		return softmax(
			self.model(
				**self.tokenizer(
					[(question, context)],
					padding=True,
					truncation=True,
					return_tensors='pt'
				)
			).logits,
			dim=-1
		).tolist()[0][1] > 0.8 # 0=P(no), 1=P(yes)