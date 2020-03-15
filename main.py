# coding: utf-8
# Updated to Python 3.X

import apache_beam as beam

import operator

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# from combiners.top import TopDistinctFn

import argparse, json, logging, time
from random import choice, randint

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import SetupOptions
import apache_beam.transforms.combiners as combine

# Default PipelineOptions()
p = beam.Pipeline(options=PipelineOptions())

# Inherit as a Class from beam.DoFn
class Transaction(beam.DoFn):
	"""
	"""
	def process(self, element):
		""" """
		date, time, id, item = element.split(',')

		return [{"date": date, "time": time, "id": id, "item": item}]


# Used to print to screen
class Printer(beam.DoFn):
	def process(self, data_item):
		print(f'\n{data_item}')


class GetTotal(beam.DoFn):
	"""
	"""
	def process(self, element):
		""" """

		# get the total transactions for one item
		return [(str(element[0]), sum(element[1]))]


class UniqList(beam.DoFn):
    def process(self, element):
        return [(element[0], list(set(element[1])))]


class GetTop10Fn(beam.DoFn):
	"""Prints Top 10 user score by num of events"""
	def process(self, element):
		# return element.sort(key=operator.itemgetter(1))

		return (sorted(element, key=lambda x: x[1], reverse=True))[:10]

class GetBottom10Fn(beam.DoFn):
	"""Prints Bottom 10 user score by num of events"""
	def process(self, element):

		return (sorted(element, key=lambda x: x[1], reverse=False))[:10]


class GetMaxFn(beam.DoFn):
	"""Prints Top 10 user score by num of events"""
	def process(self, element):
		# return element.sort(key=operator.itemgetter(1))

		return sorted(element, key=lambda x: x[1], reverse=True)[:1] # (max(element, key=lambda i: i[1])[0])

class GetMinFn(beam.DoFn):
	"""Prints Bottom 10 user score by num of events"""
	def process(self, element):

		return sorted(element, key=lambda x: x[1], reverse=False)[:1] # (min(element, key=lambda i: i[1])[0])

# Use a ParDo
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	)

# Use the ParDo object
count_of_daily_transactions = (data_from_source
	| 'Clean the item 01' >> beam.ParDo(Transaction())
	| 'Map record to 1' >> beam.Map(lambda record: (record['date'], 1))
    | 'GroupBy the data' >> beam.GroupByKey()
    | 'Get the total in each day' >> beam.ParDo(GetTotal())
    | 'Export results to day-list-count file' >> WriteToText('output/day-list-count', '.csv')

    )


list_of_daily_items = (data_from_source
	| 'Clean the item' >> beam.ParDo(Transaction())
	| 'Map the item to its date' >> beam.Map(lambda record: (record['date'], record['item']))
	| 'GroupBy the data by date' >> beam.GroupByKey()
	| 'Get Unique List' >> beam.ParDo(UniqList())
	| 'Export results to daily-items-list file' >> WriteToText('output/daily-items-list', '.txt')
	)

# Option 1 - With GroupKey
# number_of_transactions_per_item = (data_from_source
# 	| 'Clean the item for items count' >> beam.ParDo(Transaction())
# 	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
# 	| 'GroupBy the data for items count' >> beam.GroupByKey()
# 	| 'Get the total for each item' >> beam.ParDo(GetTotal())
# 	)

# Option 2 - With CombinePerKey
# number_of_transactions_per_item = (
# 	data_from_source
# 	| 'Clean the item for items count' >> beam.ParDo(Transaction())
# 	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
# 	# This can be used directly if print to screen not needed as
# 	# | >> beam.CombinePerKey(sum)
# 	| 'Get the total for each item' >> beam.CombinePerKey(sum)
# 	# To print to screen
# 	| 'Prints data to screen' >> beam.ParDo(Printer())
# 	)

# Option 3
number_of_transactions_per_item = (
	data_from_source
	| 'Clean the item for items count' >> beam.ParDo(Transaction())
	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
	| 'Get the total for each item' >> beam.CombinePerKey(sum)
	| 'Convert data into List' >> (
		beam.CombineGlobally(beam.combiners.ToListCombineFn()) # ToDictCombineFn())
		)
	)


# Uses the Dictionary
# # Maximum
# most_popular_item = (
# 	number_of_transactions_per_item
# 	| 'Get the item with maximum count' >> beam.Map(lambda item: max(item, key=item.get))
# 	# To print to screen
# 	| 'Prints max. data to screen' >> beam.ParDo(Printer())
# 	)


# # Minimum
# less_popular_item = (
# 	number_of_transactions_per_item 
# 	| 'Get the item with minimum count' >> beam.Map(lambda item: min(item, key=item.get))
# 	# # To print to screen
# 	| 'Prints min. data to screen' >> beam.ParDo(Printer())	
# 	)


# Modified to use the List
# Maximum
most_popular_item = (
	number_of_transactions_per_item
	| 'Most popular item' >> beam.ParDo(GetMaxFn())
	| 'Get the item with maximum count' >> beam.Map(lambda record: record[0])
	# To print to screen
	| 'Prints max. data to screen' >> beam.ParDo(Printer())
	)


# Minimum
less_popular_item = (
	number_of_transactions_per_item
	| 'Least popular item' >> beam.ParDo(GetMinFn())
	| 'Get the item with minimum count' >> beam.Map(lambda record: record[0])
	# # To print to screen
	| 'Prints min. data to screen' >> beam.ParDo(Printer())	
	)


# This will work using the "ToListCombineFn" from Option 3 above
# Top 10
top10 = (
	number_of_transactions_per_item
	| 'Top 10' >> beam.ParDo(GetTop10Fn())
	| 'Choose elements only from top 10' >> beam.Map(lambda record: record[0])	
	| 'Prints top 10 data to screen' >> beam.ParDo(Printer())
	)


# Bottom 10
bottom10 = (
	number_of_transactions_per_item
	| 'Bottom 10' >> beam.ParDo(GetBottom10Fn())
	| 'Choose elements only from bottom 10' >> beam.Map(lambda record: record[0])	
	| 'Prints bottom 10 data to screen' >> beam.ParDo(Printer())
	)

result = p.run()