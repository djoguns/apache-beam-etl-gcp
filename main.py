# coding: utf-8
# Updated to Python 3.X

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Generate Timestamp
import datetime
date_now = datetime.datetime.now()
# date = '2019_10_17' # yyyy_mm_dd
now = datetime.datetime.now().strftime('%Y_%m_%d-%H_%M_%S')

# Default PipelineOptions()
p = beam.Pipeline(options=PipelineOptions())

# Inherit as a Class from beam.DoFn
class GetTotal(beam.DoFn):
	"""
	"""
	def process(self, element):
		""" """

		# get the total transactions for one item
		return [(str(element[0]), sum(element[1]))]

# Use a ParDo
# Uses the beam.ParDo
data_from_source = (p

	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	
	# Using beam.Map to accept lambda function
	| 'Splitter using beam.Map' >> beam.Map(lambda record: (record.split(','))[0])
	
	# Map each record with 1 as a counter.
	| 'Map record to 1' >> beam.Map(lambda record: (record, 1))

 	# Group the records by similar data
	| 'GroupBy the data' >> beam.GroupByKey()	

	# Get the sum
	# Option 1
	| 'Get the total in each day' >> beam.ParDo(GetTotal())
	# Option 2
	# | 'Sum using beam.Map' >> beam.Map(lambda record: (record[0],sum(record[1])))

	# Export results to a new file
	# You can a timestamp, just for the fun of it.
	| 'Export results to a new file' >> WriteToText('output/day-list_' + now, '.txt')
	)

result = p.run()