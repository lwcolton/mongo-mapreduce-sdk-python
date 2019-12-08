import collections
import math
import time

import bson
import pymongo
import requests
import requests.exceptions


class MongoMapreduceAPI:
    def __init__(self, api_key=None, project_id=None, mongo_client=None):
        if type(api_key != str):
            raise ValueError("Must supply api_key, type str")
        if type(project_id != str):
            raise ValueError("Must supply project_id, type str")
        if not isinstance(mongo_client, pymongo.MongoClient):
            raise ValueError("Must supply mongo_client, type pymongo.MongoClient")
        self.session = self.get_session(api_key)
        self.project_id = project_id
        self.mongo_client = mongo_client
        self.worker_functions = None

    def get_session(self, api_key):
        session = requests.Session()
        session.timeout = (10, 10)
        session.headers.update({'x-api-key': api_key})
        return session

    def submit_job(self, function_name=None, queue=None, query=None):
        if type(function_name != str):
            raise ValueError("Must supply function_name type str")
        if query is None:
            query = {}
        url = "/api/v1/{project_id}/jobs".format(project_id=self.project_id)
        request_payload = {
            "functionName": function_name,
            "query": query
        }
        if queue:
            request_payload["queue"] = queue
        response = self.session.post(url, json=request_payload)
        return response.json()

    def list_jobs(self, completed=None, queue=None, order_by=None, page=None, perPage=None, block=False, timeout=(10,10)):
        request_payload = {}
        if completed is not None:
            request_payload["completed"] = completed

        if queue is not None:
            request_payload["queue"] = queue

        if order_by is not None:
            request_payload["order_by"] = order_by

        if page is not None:
            request_payload["page"] = page

        if perPage is not None:
            request_payload["perPage"] = perPage

        request_payload["block"] = block

        url = "/api/v1/{project_id}/jobs/list".format(project_id=self.project_id)
        response = self.session.get(url, json=request_payload, timeout=timeout)
        response_payload = response.json()
        return response_payload["jobs"]

    def get_next_job(self, queue=None):
        jobs = self.list_jobs(
            completed=False,
            queue=queue,
            order_by=[("submitted", pymongo.ASCENDING)],
            page=1,
            perPage=1,
            block=True,
            timeout=(10,1000)
        )
        return jobs[0]

    def initialize(self, job, db_name, collection_name):
        init_payload = {}
        collection_namespace = "{0}.{1}".format(db_name, collection_name)
        collections_bson = list(self.mongo_client.config.collections.find_raw_bson({"_id":collection_namespace}))
        if collections_bson:
            codec_options = bson.CodecOptions(document_class=collections.OrderedDict)
            collection_info = bson.BSON.decode(collections_bson[0], codec_options=codec_options)
            key = collection_info["key"]
            sort_keys = list(key.keys())
            sort = [(sort_key, key[sort_key]) for sort_key in sort_keys]
        else:
            sort_keys = ["_id"]
            sort = [("_id"), 1]
        major_version, minor_version, patch_version = pymongo.version_tuple
        if (major_version == 3 and minor_version >= 7) or major_version > 3:
            count = self.mongo_client[db_name][collection_name].estimated_document_count()
        else:
            count = self.mongo_client[db_name][collection_name].count()

        if count >= 10000:
            chunks = 1000
        else:
            chunks = 10
        skip = math.ceil(count / chunks)
        init_url = "/api/v1/projects/{project_id}/jobs/{job_id}/initialize".format(
            project_id = self.project_id, job_id=job["_id"]
        )
        init_response = requests.post(init_url)
        
        range_docs = []
        initialize_timeout = job["initializeTimeout"]
        start_time = int(time.time())
        update_time = start_time + (initialize_timeout / 2)
        for x in range(0,chunks):
            return_docs = list(self.mongo_client[db_name][collection_name].find().sort(sort).skip(skip*x).limit(1))
            if return_docs:
                range_doc = {key: return_docs[0][key] for key in sort_keys}
                if range_docs[-1] != range_doc:
                    range_docs.append(range_doc)
            else:
                break
            if int(time.time()) >= update_time:
                requests.patch(init_url)

        init_payload["rangeDocs"] = range_docs


    def run(self, worker_functions, queue=None, documents_per_call=100):
        worker_id = str(bson.ObjectId())
        while True:
            job = self.get_next_job()
            db_name = job["database"]
            collection_name = job["collection"]
            if not job.get("initialized"):
                self.initialize(job, db_name, collection_name)
            work_url = "/api/v1/projects/{project_id}/jobs/{job_id}/work/{worker_id}".format(
                project_id=self.project_id, job_id=job["_id"], worker_id=worker_id
            )

            try:
                work_response = self.session.get(work_url)
            except requests.exceptions.ReadTimeout:
                continue
            work_payload = work_response.json()
            function_name = work_payload["functionName"]
            do_work = worker_functions[function_name]
            query = work_payload["query"]
            work_params = work_payload["params"]
            sort = work_payload["sort"]
            mongo_query = self.mongo_client[collection_name].find(query, sort=sort)
            if documents_per_call:
                mongo_query = mongo_query.limit(documents_per_call)
            documents = list(mongo_query)
            do_work(documents, params=work_params)
            self.session.post(
                work_url,
                json={"_id": work_payload["_id"]}
            )



select master
master gets shard config or document count and calls API
API:
if sharded use shard config chunks
if not sharded docs_per_chunk = count_documents / (num_workers * 10), sort by _id ascending
api tells sdk query, sort, skip, and limit
