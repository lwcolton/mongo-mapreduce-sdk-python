.. MReduce Python SDK documentation master file, created by
   sphinx-quickstart on Sat Dec 14 18:47:37 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to MReduce Python SDK's documentation!
==============================================

API Documentation: :py:class:`mreduce.API`

Quickstart
==============================================

The following code shows how to run a map-reduce operation that will return the sum of purchases for a given customer

.. code-block:: python

   import random
   import threading

   import bson
   import pymongo

   import mreduce


   mongo_client = pymongo.MongoClient("mongodb://your_mongodb_server")
   test_collection = mogno_client.test.test_collection

   for x in range(0, 100):
       customer_id = str(bson.ObjectId())
       documents = []
       for x in rangdom.randrane(10, 100):
           price = random.randrange(0,1000)
           documents.append({"customer_id":customer_id, "price":price})
      test_collection.insert_many(documents)

   def map_func(document):
       yield document["customer_id"], document["price"]

   def reduce_func(customer_id, prices):
       return sum(prices)

   worker_functions = {
       "exampleMap": map_func,
       "exampleReduce": reduce_func
   }

   api = mreduce.API(
       api_key = "...",
       mongo_client = mongo_client
   )

   project_id = "..."

   thread = threading.Thread(
       target=api.run,
       args=[project_id, worker_functions]
   )
   thread.start()

   job = api.submit_job(
       projectId=project["_id"],
       mapFunctionName="exampleMap",
       reduceFunctionName="exampleReduce",
       inputDatabase="test",
       inputCollection="test_collection",
       outputDatabase="test",
       outputCollection="test_results"
   )
   result = job.wait_for_result()

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
