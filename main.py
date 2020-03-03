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

		return print([{"date": date, "time": time, "id": id, "item": item}])

# Use a ParDo
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	)

# Use the ParDo object
list_of_daily_items = (data_from_source
	| 'Clean the item' >> beam.ParDo(Transaction())
	| 'Map the item to its date' >> beam.Map(lambda record: (record['date'], record['item']))
	| 'GroupBy the data by date' >> beam.GroupByKey()
	)

result = p.run()