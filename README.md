# Short intro

This is an explanatory activity for the `apache-beam-etl`, based on [Medium article](https://medium.com/@selsaber/data-etl-using-apache-beam-part-one-48ca1b30b10a).

# Setup your environment

Install the environment.yml
```bash
# Install virtual env
conda env create --file environment.yml

# Activate virtual env
conda activate dflow-env
```

# Preparing the code
(Credit to - https://medium.com/@selsaber/data-etl-using-apache-beam-part-one-48ca1b30b10a)

Create your new python file, call it `counter.py` as we will do a simple counting operation. For that, I am using a simple dataset, from [Kaggle Bakery](https://www.kaggle.com/sulmansarwar/transactions-from-a-bakery) about some transactions from a bakery, which was here before, but you can still find it here. You will find it online.

The dataset contains four variables:

- Date
- Time
- Transaction ID
- Item

We simply will get some integrated information from it:

1. The count of transactions happened in different days. (Part One)
2. The list of items had been sold in different days. (Part Two)
3. The maximum and the minimum number of transactions per item. (Part Two)

Loading the data from different resources:

1. CSV/TXT (Part One)
2. CSV (Part Two)
3. BigQuery (Part Three)

Transforming the data, running the operations:

1. Map (ParDo)
2. Shuffle\Reduce (GroupBy)

Inserting the final data into the destination:

1. TXT (Part One)
2. CSV (Part Two)
3. BigQuery (Part Three)

First, we need to create and configure our [pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline), which will encapsulate all the data and steps in our data processing task.

# Part One

## Pipeline configurations

Import the required libraries, basically beam, and its configuration options (this will be done withing our main file - `main.py`):

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
p = beam.Pipeline(options=PipelineOptions())
```

For the configuration, you can have your own configuration by creating an element from the PipelineOptions class:

```python
class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(' — input',
                      help='Input for the pipeline',
                      default='gs://my-bucket/input')
    parser.add_argument(' — output',
                     help='Output for the pipeline',
                     default='gs://my-bucket/output')
```

In the example here, for input and output, the default path pointing to a file in Google Cloud Storage, you can change it with your default URL:

```python
parser.add_argument(' — input',
                     help='Input for the pipeline',
                     default='https://your/path/to/the/input')
```

We will see how to load the local files soon. Then you can use these options in creating your pipeline:

```python
p = beam.Pipeline(options=MyOptions)
```

## Reading the input data

Now, let’s read the input data from a file… First, import the text reader and writer from apache_beam.io

```python
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
```

Then the first thing that our pipeline will do is loading the data from the source file:

```python
data_from_source = (p
 | 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
 )
```

Here we read the CSV file as text, it is now stored in the `data_from_source` variable.

Now, how to do some processing on this data within apache beam? As we mentioned earlier, the `Map\Shuffle\Reduce` here will be implemented here by `ParDo`, `GroupByKey`, and `CoGroupByKey`… which are part of the core Beam transformation…

## How to write a ParDo?

There two ways to write your ParDo in beam:

1. Define it as a function:

```python
def printer(data_item):
	print(data_item)
```

And use it like this:

```python
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	| 'Print the data' >> beam.ParDo(printer)
)
```

2. Inherit as a class from beam.DoFn:

```python
class Printer(beam.DoFn):

	def process(self, data_item):

		print(data_item)
```

And use it like:

```python
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	| 'Print the data' >> beam.ParDo(Printer())
	)
```

## Calling the ParDo

The two will do exactly the same thing, but you call them in different ways as we saw.

Now our file looks like:

```python
# coding: utf-8
# Updated to Python 3.X

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

p = beam.Pipeline(options=PipelineOptions())

class Printer(beam.DoFn):

	def process(self, data_item):

		print(data_item)

def printer(data_item):

	print(data_item)


data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	| 'Print the data' >> beam.ParDo(printer)
)

result = p.run()
```

If we run this file, it will just print the data from the source to our terminal.

Please notice that we used the default PipelineOptions() here, as we don’t have specific options to set for now.

Sample run as follows:

```python
python main.py
```

```
# Sample output
.
..
...
2017-02-02,15:44:42,5817,Coffee
2017-02-02,15:58:52,5818,Coffee
2017-02-02,15:58:52,5818,Coffee
2017-02-02,15:58:52,5818,Juice
2017-02-02,15:58:52,5818,Cake
...
..
.
```

## Running the Transformation on the data

We are reading the raw data to process it… so let’s see how to manipulate the data and extract more meaningful statistics from it.

When working with the pipeline, the data sets we have called **`PCollection`**, and the operations we run called **`Transformation`**.
As we noticed that the data is processed line by line. That is because **`ReadFromText`** generates a PCollection as an output, where each element in the output PCollection represents one line of text from the input file.

We used **`ParDo`** function to run a specific operation on the data, this operation run on every element in the PCollection. As we saw, we printed every line of the data alone.

Now, we will only get the dates from the input data. To do that, we will have a new **`DoFn`** to return the date only from each element.

If you check the type of the data passed from the PCollection to the DoFn from the ParDo, it will return `<type ‘unicode’>` which we will treat as a string. To check that, create and use the next DoFn:

```python
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
```

This displays something like this:

```
.
..
...
<class 'str'>
<class 'str'>
<class 'str'>
<class 'str'>
<class 'str'>
<class 'str'>
<class 'str'>
...
..
.
```

it is a string. We can split it by the comma operator, which will return an array of elements. We can easily use `beam.Map` which accepts `lambda` function and implements it on every record of the PCollection.

Our lambda function is simple here, split the input by the comma operator, then return the first element:

```python
beam.Map(lambda record: (record.split(‘,’))[0])
```

Our pipeline will then look like this:

```python
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	| 'Splitter using beam.Map' >> beam.Map(lambda record: (record.split(','))[0])
	| 'Print the date' >> beam.ParDo(Printer())
	)
```

The output should be something like this:

```
.
..
...
2017-04-09
2017-04-09
2017-04-09
2017-04-09
2017-04-09
2017-04-09
2017-04-09
...
..
.
```

Great, now we have only the dates. What else can we do with this data?

Let’s count how many orders made everyday. To do that, we will need to do three simple steps.

1. Map each record with 1 as a counter.
2. Group the records with the similar data.
3. Get the sum of the 1s.

### Map each record

```python
	| 'Map record to 1' >> beam.Map(lambda record: (record, 1))
```

This should output:

```
.
..
...
(u'2017–04–09', 1)
(u'2017–04–09', 1)
(u'2017–04–09', 1)
..
.
```

### Group the records by similar data

```python
	| 'GroupBy the data' >> beam.GroupByKey()
```

Outputs:

```
(u'2016–11–24', [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
```

### Get the sum

We have two ways to do this - `beam.Map` or `beam.ParDo`

```python
	| 'Sum using beam.Map' >> beam.Map(lambda record: (record[0],sum(record[1])))
```

Alternatively, using `ParDo`:

```python
class GetTotal(beam.DoFn):
	"""
	"""
	def process(self, element):
		""" """

		# get the total transactions for one item
		return [(str(element[0]),sum(element[1]))]
	...
	...

# In your pipeline
	| 'Get the total in each day' >> beam.ParDo(GetTotal())
```

Putting the steps together, we have:

```python
# Inherit as a Class from beam.DoFn
class GetTotal(beam.DoFn):
	"""
	"""
	def process(self, element):
		""" """

		# get the total transactions for one item
		return print([(str(element[0]), sum(element[1]))])

# Use a ParDo
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	
	# Using beam.Map to accept lambda function
	| 'Splitter using beam.Map' >> beam.Map(lambda record: (record.split(','))[0])
	
	# Map each record with 1 as a counter.
	| 'Map record to 1' >> beam.Map(lambda record: (record, 1))

 	# Group the records by similar data
	| 'GroupBy the data' >> beam.GroupByKey()	

	# Get the sum
	| 'Get the total in each day' >> beam.ParDo(GetTotal())
	)

result = p.run()
```

After running the application, we get a similar output as follows:

```
.
..
...
[('2017-04-03', 121)]
[('2017-04-04', 120)]
[('2017-04-05', 145)]
[('2017-04-06', 119)]
[('2017-04-07', 103)]
[('2017-04-08', 209)]
[('2017-04-09', 72)]
```

## Writing the output data

We need to store this data somewhere for future usage, so we will write it in a new text file for now.

```python
	| 'Export results to new file' >> WriteToText('output/day-list', '.txt')
```

Using `beam.Map`, the code should look like:

```python
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
	# | 'Get the total in each day' >> beam.ParDo(GetTotal())
	# Option 2
	| 'Sum using beam.Map' >> beam.Map(lambda record: (record[0],sum(record[1])))

	# Export results to a new file
	| 'Export results to a new file' >> WriteToText('output/day-list', '.txt')
	)
```

And using `ParDo`, we should have:

```python
class GetTotal(beam.DoFn):
	"""
	"""
	def process(self, element):
		""" """

		# get the total transactions for one item
		return print([(str(element[0]), sum(element[1]))])

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
	| 'Export results to a new file' >> WriteToText('output/day-list-pardo', '.txt')
	)
```

# Part Two

In this part, we will achieve the following:

1. Getting a unique list of items that had been sold in different days.
2. Getting the maximum and the minimum number of transactions per item, to get the most and less popular items.
3. More built-in transformation functions (**`CombinePerKey`** — **`CombineGlobally`**).

## Code Cleaning

We will enhance the quality of our code. First, we will read the data from the CSV and will in a totally separated step.

```python
data_from_source = (p
	| 'ReadMyFile' >> ReadFromText('input/BreadBasket_DMS.csv')
	)
```

The `data_from_source` can be used in many different operations as shown below:

```python
operation_001 = (data_from_source
	| 'some operations 0011' >> ...
)
operation_002 = (data_from_source
	| 'some operations 0021' >> ...
)
```

## Data Transforming

We will create a `DoFn` that convert the data for something like an object, so we can play around with it easily. (Python dictionary if you know it).


```python
class Transaction(beam.DoFn):
	"""
	"""
	def process(self, element):
		""" """
		(date, time, id, item) = element.split(',')

		return [{"date": date, "time": time, "id": id, "item": item}]
```

So we can easily after that map the items to the date, and group the results by date:

```python
# Use the ParDo object
list_of_daily_items = (data_from_source
	| 'Clean the item' >> beam.ParDo(Transaction())
	| 'Map the item to its date' >> beam.Map(lambda record: (record['date'], record['item']))
	| 'GroupBy the data by date' >> beam.GroupByKey()
	)
```

This will generate something looks like this:

```
...
...
[{'date': '2017-04-09', 'time': '14:32:58', 'id': '9682', 'item': 'Tacos/Fajita'}]
[{'date': '2017-04-09', 'time': '14:32:58', 'id': '9682', 'item': 'Coffee'}]
[{'date': '2017-04-09', 'time': '14:32:58', 'id': '9682', 'item': 'Tea'}]
[{'date': '2017-04-09', 'time': '14:57:06', 'id': '9683', 'item': 'Coffee'}]
[{'date': '2017-04-09', 'time': '14:57:06', 'id': '9683', 'item': 'Pastry'}]
[{'date': '2017-04-09', 'time': '15:04:24', 'id': '9684', 'item': 'Smoothies'}]
```

Creating a final code for this, we have:

```python
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
	| 'Export results to daily-items-list file' >> WriteToText('output/daily-items-list', '.txt')
	)

result = p.run()
```

Sample Output is shown below:

```
('Date', ['Item'])
('2016-10-30', ['Bread', 'Scandinavian', 'Scandinavian', 'Hot chocolate', 'Jam', 'Cookies', 'Muffin', 'Coffee', 'Pastry', 'Bread', 'Medialuna', 'Pastry', 'Muffin', 'Medialuna', 'Pastry', 'Coffee', 'Tea', 'Pastry', 'Bread', 'Bread', 'Muffin', 'Scandinavian', 'Medialuna', 'Bread', 'Medialuna', 'Bread', 'NONE', 'Jam', 'Coffee', 'Tartine', 'Pastry', 'Tea', 'Basket', 'Bread', 'Coffee', 'Bread', 'Medialuna', 'Pastry', 'NONE', 'NONE', 'Mineral water', ...
...
...
...
```

# Exploring Most and Least Popular item

We will create a new operation to get the `maximum` and the `minimum` number of transactions happened for each item in all the data. To do that, we need first to count the appearance of each item in the whole data set. Then we will get the `max` and `min`.

We will reuse the **`data_from_source`** again as a source for our operation. And will map the item to 1 every time it appears. It is the same approach we took for counting the daily transactions but will do it on the item.

We will use the same **`GetTotal`** `DoFn` we created last time:

```python
class GetTotal(beam.DoFn):
	def process(self, element):

	# get the total transactions for one item
	return [(str(element[0]), sum(element[1]))]
```

We can then have something like this to get the total count for each item:

```python
number_of_transactions_per_item = (data_from_source
	| 'Clean the item for items count' >> beam.ParDo(Transaction())
	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
	| 'GroupBy the data for items count' >> beam.GroupByKey()
	| 'Get the total for each item' >> beam.ParDo(GetTotal())
	)
```

Or we can use some built-in features like `CombinePerKey` to do the same:

```python
number_of_transactions_per_item = (data_from_source
	| 'Clean the item for items count' >> beam.ParDo(Transaction())
	| 'Map record item to 1 for items count' >> beam.Map(lambda record: (record['item'], 1))
	| beam.CombinePerKey(sum)
	)
```

Both code snippet will return the same result.

**`CombinePerKey`** will do something similar to **`GroupByKey`**, but will also apply some logic to the operation as you can see. It is doing the same logic we wrote in the **`GetTotal`** `DoFn` before.

The output from any of them will look like (a print screen `ParDo` has been to display the output):

```
...
...
[('Half slice Monster ', 6)]
[('Gift voucher', 1)]
[('Cherry me Dried fruit', 3)]
[('Mortimer', 5)]
[('Raw bars', 1)]
[('Tacos/Fajita', 11)]
```

We need to check the maximum and the minimum within all these items, which can not be achieved if we keep treating it as a **`PCollection`**, and working on each data element as a single item.

One solution is to convert the data we have to a dictionary. That will work as long as we work on 2-element tuples. In our case we have the element name as a key and its count as a value. Which are great.

We will use another built-in function **`ToDictCombineFn`** which come under **`CombineGlobally`** and it will convert all our data to a dictionary.

```python
	| 'Convert data into Dictionary' >> (
		beam.CombineGlobally(beam.combiners.ToDictCombineFn())
		)
 ```

That will return the data looks like:

```
{'Item': 1, 'Bread': 3325, 'Scandinavian': 277, 'Hot chocolate': 590, 'Jam': 149, 'Cookies': 540, 'Muffin': 370, 'Coffee': 5471, ..., ..., ..., 'Christmas common': 11, 'Argentina Night': 7, 'Half slice Monster ': 6, 'Gift voucher': 1, 'Cherry me Dried fruit': 3, 'Mortimer': 5, 'Raw bars': 1, 'Tacos/Fajita': 11}
```

We can then play with this to extract the maximum and minimum.

As we used the data_from_source multiple times, we will use the `number_of_transactions_per_item` couple of times. One to get the maximum, and another to get the minimum.

```python
most_popular_item = (
	number_of_transactions_per_item
	| 'Get the item with maximum count' >> beam.Map(lambda item: max(item, key=item.get))
	)
```

```python
less_popular_item = (
	number_of_transactions_per_item 
	| 'Get the item with minimum count' >> beam.Map(lambda item: min(item, key=item.get))
	)
```

As you must have noticed, we habe not considered multiple items with the same maximum or minimum count. So if you want to return a list of the items which have the maximum number, or the top 10 as an example, you will need to create a separate `DoFn`.

We can use **`beam.combiners.ToListCombineFn()`** to convert the data to a list and work on it.

The following code snippet shows how to get the top and bottom elements. But ensure to use it with `Option 3` from the main code (this combines the elements to list)

```python
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

```

This [link](https://stackoverflow.com/questions/56616576/apache-beam-python-how-to-get-the-top-10-elements-of-a-pcollection-with-accu
) might give some insight.
