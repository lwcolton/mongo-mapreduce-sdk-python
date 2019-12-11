import collections
import math
import time

import bson
import pymongo
import pymongo.errors
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
        update_time = int(time.time()) + (initialize_timeout / 2)
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
                update_time = int(time.time()) + (initialize_timeout / 2)
        init_put_payload = {
            "ranges": range_docs
        }
        self.session.put(init_url, json=init_put_payload)

    def cleanup(self, projectId, job):
        map_stage = job["stages"][0]
        self.mongo_client[map_stage["outputDatabase"]][job["tempCollectionName"]].drop()
        cleanup_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/cleanup".format(
                projectId=projectId, jobId=job["_id"]
            )
        )
        self.session.post(cleanup_url)


    def run(self, projectId, worker_functions, queue=None, batch_size=100):
        workerId = str(bson.ObjectId())
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
                self.initialize(projectId, job, workerId)
                continue
            elif work_get_payload["action"] == "cleanup":
                self.cleanup(projectId, job)
                continue
            range_url = self.get_url(
                "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageIndex}/ranges/{rangeIndex}".format(
                    projectId=projectId,
                    jobId=job["_id"],
                    stageIndex=job["currentStageIndex"],
                    rangeIndex=work_get_payload["rangeIndex"]
                )
            )
            work_timeout = job["workTimeout"]
            previousInitializingAtEpoch = job["stages"][job["currentStageIndex"]]["initializingAtEpoch"]
            update_time = previousInitializingAtEpoch + (work_timeout / 2)
            post_response = self.session.post(range_url, json={"workerId": workerId})
            if post_response.status_code == 204:
                continue
            stage = job["stages"][job["currentStageIndex"]]
            filter = {}
            sort = []
            if work_get_payload["action"] == "map":
                filter = job.get("filter", {})
                sort = job.get("sort", [])
            elif work_get_payload["action"] == "reduce":
                map_ranges_url = "/api/v1/projects/{projectId}/jobs/{jobId}/stages/0/ranges".format(
                    projectId=projectId, jobId=job["_id"]
                )
                map_ranges_response = self.session.get(map_ranges_url)
                map_ranges_payload = map_ranges_response.json()
                valid_result_ids = [map_range["resultId"] for map_range in map_ranges_payload["ranges"]]
                filter = {"resultId":{"$in":valid_result_ids}}
                sort = [("key", pymongo.ASCENDING)]

            rangeStart = work_get_payload.get("rangeStart")
            rangeEnd = work_get_payload["rangeEnd"]
            filter["$and"].setdefault([])
            for range_key in rangeEnd.keys():
                if rangeStart:
                    filter["$and"].append({range_key:{"$gte":rangeStart[range_key]}})
                filter["$and"].append({range_key:{"$lt":rangeEnd[range_key]}})
            cursor = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].find(
                filter,
                batch_size=batch_size
            ).sort(sort)
            do_work_function = worker_functions[stage["functionName"]]
            continue_working = True
            reduce_key = None
            reduce_values = []
            resultId = str(bson.ObjectId)
            while continue_working:
                if int(time.time()) > update_time:
                    patch_response = self.session.patch(
                        range_url,
                        json={"workerId": workerId})
                    if patch_response.status_code == 204:
                        break
                    patch_response_body = patch_response.json()
                    job = patch_response_body["job"]
                    stage = job["stages"][job["currentStageIndex"]]
                    previousInitializingAtEpoch = stage["initializingAtEpoch"]
                    update_time = previousInitializingAtEpoch + (work_timeout / 2)
                documents = []
                insert_docs = []
                for x in range(0, batch_size):
                    try:
                        documents.append(cursor.next())
                    except StopIteration:
                        continue_working = False
                        break
                if work_get_payload["action"] == "map":
                    output_docs = {}
                    results = do_work_function(documents)
                    for key, value in results:
                        output_docs.setdefault(key, [])
                        output_docs[key].append(value)
                    output_keys = list(output_docs.keys())
                    for key in output_keys:
                        values = output_docs[key]
                        insert_docs.append({"key": key, values: values})
                elif work_get_payload["action"] == "reduce":
                    for doc in documents:
                        if doc["key"] != reduce_key:
                            reduced_value = do_work_function(reduce_key, reduce_values)
                            insert_docs.append({"_id": reduce_key, "value": reduced_value})
                            reduce_values = [doc["value"]]
                            reduce_key = doc["key"]
                        else:
                            reduce_values.append(doc["value"])
                    if len(reduce_values) > 1:
                        reduce_values = [do_work_function(reduce_values)]
                elif work_get_payload["action"] == "finalize":
                    for doc in documents:
                        finalized_value = do_work_function(doc["_id"], doc["value"])
                        self.mongo_client[stage["outputDatabase"]][stage["outputCollection"]].find_one_and_update(
                            {
                                "_id":doc["_id"],
                                "finalized":{"$exists":False}
                            },
                            {
                                "$set":{
                                    "value": finalized_value,
                                    "finalized": True
                                }
                            }
                        )
                if insert_docs:
                    for doc in insert_docs:
                        doc["resultId"] = resultId
                    try:
                        self.mongo_client[stage["outputDatabase"]][stage["outputCollection"]].insert_many(insert_docs)
                    except pymongo.errors.DuplicateKeyError as duplicate_key_error:
                        pass
            put_response = self.session.put(range_url, json={"resultId": resultId})