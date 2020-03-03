# coding: utf-8
# Updated to Python 3.X

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

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

# Use a ParDo
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	)

# Use the ParDo object
# Option 1 - With GroupKey
# number_of_transactions_per_item = (data_from_source
# 	| 'Clean the item for items count' >> beam.ParDo(Transaction())
# 	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
# 	| 'GroupBy the data for items count' >> beam.GroupByKey()
# 	| 'Get the total for each item' >> beam.ParDo(GetTotal())
# 	)

# Option 2 - With CombinePerKey
# number_of_transactions_per_item = (data_from_source
# 	| 'Clean the item for items count' >> beam.ParDo(Transaction())
# 	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
# 	# This can be used directly if print to screen not needed as
# 	# | >> beam.CombinePerKey(sum)
# 	| 'Get the total for each item' >> beam.CombinePerKey(sum)
# 	# To print to screen
# 	| 'Prints data to screen' >> beam.ParDo(Printer())

# 	)

# Option 3
number_of_transactions_per_item = (data_from_source
	| 'Clean the item for items count' >> beam.ParDo(Transaction())
	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
	| 'Get the total for each item' >> beam.CombinePerKey(sum)
	| 'Convert data into Dictionary' >> (
		beam.CombineGlobally(beam.combiners.ToDictCombineFn())
		)
	)

# Maximum
most_popular_item = (
	number_of_transactions_per_item
	| 'Get the item with maximum count' >> beam.Map(lambda item: max(item, key=item.get))
	# To print to screen
	| 'Prints max. data to screen' >> beam.ParDo(Printer())	
	)

# Minimum
less_popular_item = (
	number_of_transactions_per_item 
	| 'Get the item with minimum count' >> beam.Map(lambda item: min(item, key=item.get))
	# # To print to screen
	| 'Prints min. data to screen' >> beam.ParDo(Printer())	
	)

result = p.run()