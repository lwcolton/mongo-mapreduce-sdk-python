import collections
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

    def get_session(self, api_key, timeout=(10,10)):
        session = requests.Session()
        session.timeout = timeout
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

    def list_jobs(self, projectId=None, filter=None, sort=None, page=None, perPage=None, timeout=(10,10)):
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

    def get_job(self, projectId, jobId):
        url = "/api/v1/{projectId}/jobs/{jobId}".format(projectId=projectId, jobId=jobId)
        response = self.session.get(url)
        return response.json()

    def initialize(self, projectId, job, worker_id):
        stageIndex = job["currentStageIndex"]
        init_url = "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageIndex}/initialize".format(
            projectId = projectId, jobId=job["_id"], stageIndex=stageIndex
        )
        init_post_response = self.session.post(init_url, json={"workerId":worker_id})
        if init_post_response.status_code == 204:
            return
        init_post_response_body = init_post_response.json()
        stage = init_post_response_body["job"]["stages"][stageIndex]
        collection_namespace = "{0}.{1}".format(stage["inputDatabase"], stage["inputCollection"])
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
            count = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].estimated_document_count()
        else:
            count = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].count()

        if count >= 10000:
            chunks = 1000
        else:
            chunks = 100
        skip = math.ceil(count / chunks)

        range_docs = []
        initialize_timeout = job["initializeTimeout"]
        start_time = int(time.time())
        update_time = start_time + (initialize_timeout / 2)
        collection = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]]
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

        init_put_payload = {
            "ranges": range_docs
        }
        self.session.put(init_url, json=init_put_payload)

    def run(self, projectId, worker_functions, queue=None, documents_per_call=100):
        worker_id = str(bson.ObjectId())
        while True:
            work_url = "/api/v1/projects/{projectId}/work".format(
                projectId=projectId
            )
            params = {}
            if queue:
                params[queue] = queue
            work_get_response = self.session.get(work_url, params=params)
            while work_get_response.status_code == 204:
                time.sleep(10)
                work_get_response = self.session.get(work_url, params=params)
            work_get_payload = work_get_response.json()
            job = work_get_payload["job"]
            if work_get_payload["action"] == "initialize":
                self.initialize(projectId, job, worker_id)
                continue





            work_params = job["params"]
            sort = work_payload["sort"]
            range_id = str(bson.ObjectId())
            mapFunction = worker_functions[job["mapFunctionName"]]
            reduceFunction = worker_functions.get(job["reduceFunctionName"])
            if work_payload["stage"] == "map":
                mongo_query = self.mongo_client[job["inputDatabase"]][job["inputCollection"]].find(
                    work_payload["query"], sort=sort
                )
            elif work_payload["stage"] == "reduce":
                mongo_query = self.mongo_client[job["tempDatabase"]][job["tempCollection"]].find(
                    sort=("key", 1)
                )
            else:

            more_work = True
            while more_work:

                documents = []
                for x in range(0, documents_per_call):
                    try:
                        documents.append(next(mongo_query))
                    except StopIteration:
                        more_work = False
                if documents:
                    results = do_work(documents, params=work_params)
                    if results and job["outputCollection"]:
                        reduce_function = worker_functions[job["reduceFunction"]]
                        if work_payload["stage"] == "map":
                            output_docs = {}
                            for key, value in results:
                                output_docs.setdefault(key, [])
                                output_docs[key].append(value)
                            output_keys = list(output_docs.keys())
                            for key in output_keys:
                                values = output_docs[key]
                                if len(values) > 1:
                                    result = reduce_function(values)
                                    output_docs[key] = [result]
                            insert_docs = []
                            for key in output_keys:
                                values = output_docs[key]
                                insert_docs.append({"key":key, values: values, "rangeId": range_id})
                            self.mongo_client[job["tempDatabase"]][job["tempCollection"]].insert_many(insert_docs)
            self.session.post(
                work_url,
                json={"rangeProcessed": {"index": work_payload["index"]}}
            )