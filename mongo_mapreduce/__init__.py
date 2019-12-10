import collections
import copy
import math
import time

import bson
import pymongo
import requests
import requests.exceptions


class MongoMapreduceAPI:
    def __init__(self, api_key=None, mongo_client=None, host="localhost"):
        if type(api_key) != str:
            raise ValueError("Must supply api_key, type str")
        if not isinstance(mongo_client, pymongo.MongoClient):
            raise ValueError("Must supply mongo_client, type pymongo.MongoClient")
        self.host = host
        self.session = self.get_session(api_key)
        self.mongo_client = mongo_client
        self.worker_functions = None

    def get_session(self, api_key):
        session = requests.Session()
        session.timeout = (10, 10)
        session.headers.update({'x-api-key': api_key})
        return session

    def get_url(self, path):
        return "https://{0}{1}".format(self.host, path)

    def create_project(self, name):
        response = self.session.post(self.get_url("/api/v1/projects"), json={"name":name})
        response.raise_for_status()
        return response.json()

    def submit_job(self, projectId=None,  functionName=None, database=None, collection=None, queue=None, query=None, outputCollection=None, outputIndexes=None):
        if type(projectId) != str or not projectId:
            raise ValueError("Must supply projectId argument, type string")
        if type(functionName) != str or not functionName:
            raise ValueError("Must supply functionName argument, type string")
        if type(database) != str or not database:
            raise ValueError("Must supply database argument, type string")
        if type(collection) != str or not collection:
            raise ValueError("Must supply collection argument, type string")
        if query is None:
            query = {}
        url = self.get_url("/api/v1/projects/{projectId}/jobs".format(projectId=projectId))
        request_payload = {
            "database": database,
            "collection": collection,
            "functionName": functionName
        }

        if queue:
            request_payload["queue"] = queue
        if query:
            request_payload["query"] = query
        if outputCollection:
            request_payload["outputCollection"] = outputCollection
        if outputIndexes:
            request_payload["outputIndexes"] = outputIndexes
        response = self.session.post(url, json=request_payload)
        response.raise_for_status()
        return response.json()

    def list_jobs(self, projectId=None, filter=None, sort=None, page=None, perPage=None, block=False, timeout=(10,10)):
        if type(projectId) != str or not projectId:
            raise ValueError("Must supply projectId type str")
        request_payload = {}
        if filter is not None:
            request_payload["filter"] = filter

        if sort is not None:
            request_payload["sort"] = sort

        if page is not None:
            request_payload["page"] = page

        if perPage is not None:
            request_payload["perPage"] = perPage

        url = self.get_url("/api/v1/projects/{projectId}/jobs_search".format(projectId=projectId))
        response = self.session.post(url, json=request_payload, timeout=timeout)
        response.raise_for_status()
        response_payload = response.json()
        return response_payload["jobs"]

    def get_next_job(self, queue=None):
        filter = {
            "completed": False
        }
        if queue is not None:
            filter["queue"] = queue
        jobs = self.list_jobs(
            filter=filter,
            sort=[("submittedAtEpoch", pymongo.ASCENDING)],
            page=1,
            perPage=1,
        )
        if jobs:
            return jobs[0]
        else:
            return None

    def get_job(self, job_id):
        url = "/api/v1/{project_id}/jobs/{job_id}".format(project_id=self.project_id, job_id=job_id)
        response = self.session.get(url)
        return response.json()


    def initialize(self, job, db_name, collection_name, worker_id):
        init_url = "/api/v1/projects/{project_id}/jobs/{job_id}/initialize".format(
            project_id = self.project_id, job_id=job["_id"]
        )
        while True:
            init_post_response = self.session.post(init_url)
            init_post_response_body = init_post_response.json()
            initialized = init_post_response_body["initialized"]
            if initialized:
                return
            elif init_post_response_body["workerId"] == worker_id:
                break
            time.sleep(5)

        init_payload = {}
        collection_namespace = "{0}.{1}".format(db_name, collection_name)
        collections_bson = list(self.mongo_client.config.collections.find_raw_bson({"_id":collection_namespace}))
        init_query = None
        if collections_bson:
            codec_options = bson.CodecOptions(document_class=collections.OrderedDict)
            collection_info = bson.BSON.decode(collections_bson[0], codec_options=codec_options)
            key = collection_info["key"]
            sort_keys = list(key.keys())
            sort = [(sort_key, key[sort_key]) for sort_key in sort_keys]
            if sort_keys[0] in job["query"]:
                init_query = {}
                for index_key in sort_keys:
                    if index_key in job["query"]:
                        init_query[index_key] = job["query"][index_key]
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
            chunks = 100
        skip = math.ceil(count / chunks)

        range_docs = []
        initialize_timeout = job["initializeTimeout"]
        start_time = int(time.time())
        update_time = start_time + (initialize_timeout / 2)
        collection = self.mongo_client[db_name][collection_name]
        for x in range(0,chunks):
            return_docs = list(collection.find(filter=init_query).sort(sort).skip(skip*x).limit(1))
            if return_docs:
                range_doc = {key: return_docs[0][key] for key in sort_keys}
                if range_docs[-1] != range_doc:
                    range_docs.append(range_doc)
            else:
                break
            if int(time.time()) >= update_time:
                self.session.patch(init_url)

        init_payload["ranges"] = range_docs
        self.session.put(init_url, json=init_payload)

    def run(self, worker_functions, queue=None, documents_per_call=20):
        worker_id = str(bson.ObjectId())
        while True:
            job = self.get_next_job(queue=queue)
            while job is None:
                time.sleep(10)
                job = self.get_next_job(queue=queue)
            db_name = job["database"]
            collection_name = job["collection"]
            if not job.get("initialized"):
                self.initialize(job, db_name, collection_name, worker_id)
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
            more_work = True
            while more_work:
                range_id = str(bson.ObjectId())
                documents = []
                for x in range(0, documents_per_call):
                    try:
                        documents.append(next(mongo_query))
                    except StopIteration:
                        more_work = False
                if documents:
                    results = do_work(documents, params=work_params)
                    if job["outputCollection"] and results:
                        results = copy.deepcopy(results)
                        for result in results:
                            result["rangeId"] = range_id
                        self.mongo_client[db_name][job["outputCollection"]].insert_many(results)
            self.session.post(
                work_url,
                json={"rangeProcessed": {"index": work_payload["index"]}}
            )