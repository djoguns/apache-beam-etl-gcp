import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

p = beam.Pipeline(options=PipelineOptions())


class GetTotal(beam.DoFn):
    def process(self, element):
        # get the total transactions for one item
        return [(str(element[0]), sum(element[1]))]


class Transaction(beam.DoFn):
    def process(self, element):
        date, time, id, item = element.split(',')
        return [{"date": date, "time": time, "id": id, "item": item}]


class UniqList(beam.DoFn):
    def process(self, element):
        return [(element[0], list(set(element[1])))]


data_from_source = (p
                    | 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
                    )

count_of_daily_transactions = (data_from_source
                               | 'Clean the item 01' >> beam.ParDo(Transaction())
                               | 'Map record to 1' >> beam.Map(lambda record: (record['date'], 1))
                               | 'GroupBy the data' >> beam.GroupByKey()
                               | 'Get the total in each day' >> beam.ParDo(GetTotal())
                               | 'Export results to day-list-count file' >> WriteToText('output/day-list-count-aux', '.csv')
                               )

list_of_daily_items = (data_from_source
                       | 'Clean the item' >> beam.ParDo(Transaction())
                       | 'Map the item to its date' >> beam.Map(lambda record: (record['date'], record['item']))
                       | 'GroupBy the data by date' >> beam.GroupByKey()
                       | 'Get Unique List' >> beam.ParDo(UniqList())
                       | 'Export results to daily-items-list file' >> WriteToText('output/daily-items-list-aux', '.txt')
                       )

number_of_transactions_per_item = (data_from_source
                                   | 'Clean the item for max min' >> beam.ParDo(Transaction())
                                   | 'Map record item to 1 for max min' >> beam.Map(lambda record: (record['item'], 1))
                                   | 'Get the total for each item' >> beam.CombinePerKey(sum)
                                   | 'Convert data into Dictionary' >>
                                   beam.CombineGlobally(beam.combiners.ToDictCombineFn())
                                   )

most_popular_item = (number_of_transactions_per_item
                     | 'Get the item with maximum count' >> beam.Map(lambda item: max(item, key=item.get))
                     # | 'Export tha data for other destination or store it in a file'
                     | 'Export results to daily-items-list file - max' >> WriteToText('output/daily-items-max-aux', '.txt')
                     )

less_popular_item = (number_of_transactions_per_item
                     | 'Get the item with minimum count' >> beam.Map(lambda item: min(item, key=item.get))
                     # | 'Export tha data for other destination or store it in a file
                     | 'Export results to daily-items-list file - min' >> WriteToText('output/daily-items-min-aux', '.txt')
                     )

result = p.run()