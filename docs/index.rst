Welcome to MReduce Python SDK's documentation!
==============================================

API Documentation: :py:class:`mreduce.API`

Installation
===============================================
.. code-block:: shell

    pip install mreduce

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
    for key, value in result:
        print("Key: " + key, ", Value: " + str(value))

Note
===========================
MReduce guarantees at-least-once delivery.  That is in the event of server failure your function may be called multiple times with
the same input document(s).

Running a maintenance task
============================

MReduce can be used to run maintenance tasks, in addition to map reduce jobs.  To run a maintenance task simply omit
the 'reduceFunction', 'finalizeFunction', 'outputDatabase', and 'outputCollection' parameters.

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

    def billCustomer(document):
        # Bill customer for document["price"]
        pass


    worker_functions = {
        "billCustomer": billCustomer
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
        mapFunctionName="billCustomer",
        inputDatabase="test",
        inputCollection="test_collection",
    )

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
