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