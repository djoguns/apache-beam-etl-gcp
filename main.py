# coding: utf-8
# Updated to Python 3.X

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Default PipelineOptions()
p = beam.Pipeline(options=PipelineOptions())

# write a ParDo
# Inherit as a Class from beam.DoFn
class TypeOf(beam.DoFn):
	""" """
	def process(self, data_item):
		""" """
		print(type(data_item))

data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	# This ParDo "gives the types of the dataset"
	| 'Check data type' >> beam.ParDo(TypeOf())
	)

result = p.run()
