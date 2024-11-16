from ... import Classifier

from torch import cuda
from transformers import pipeline

# adapted from https://huggingface.co/spaces/Manish-4007/topic_suggest/blob/main/app.py
TOPICS = ["Business", "Finance", "Health", "Sports", "Politics", "Government",
	"Science", "Education", "Travel", "Technology", "Entertainment", "Environment",
	"News & Media", "Space, Universe & Cosmos", "Fashion", "Law & Crime"]


class ComprehendIt(Classifier):
	def __init__(self, base = "knowledgator/comprehend_it-base"):
		"""This generates the tokenizer and model for classifying the crawls"""
		if not cuda.is_available():
			print("Warning: No GPU available")
		self.classifier = pipeline("zero-shot-classification",
			model="knowledgator/comprehend_it-base",
			device_map="auto"
		)

	def classify(self, context, topics = TOPICS):
		result = classifier(context, topics, multi_label=True)
		return [
			label
			for label,score in zip(result["labels"], result["scores"])
			if score >= 0.8
		]