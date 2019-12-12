import collections
import logging
import math
import time

import bson
import pymongo
import pymongo.errors
import requests
import requests.exceptions


class MongoMapreduceAPI:
    def __init__(self, api_key=None, mongo_client=None, host="localhost", logger=None):
        if type(api_key) != str:
            raise ValueError("Must supply api_key, type str")
        if not isinstance(mongo_client, pymongo.MongoClient):
            raise ValueError("Must supply mongo_client, type pymongo.MongoClient")
        self.host = host
        self.session = self.get_session(api_key)
        self.mongo_client = mongo_client
        self.worker_functions = None
        if logger is None:
            logger = logging.getLogger(__name__)
        self.logger = logger

    def get_session(self, api_key, timeout=(10,10)):
        session = requests.Session()
        session.timeout = timeout
        session.headers.update({'x-api-key': api_key})
        return session

    def get_url(self, path):
        return "https://{0}{1}".format(self.host, path)

    def api_call(self, method, *args, **kwargs):
        method_function = getattr(self.session, method.lower())
        response = method_function(*args, **kwargs)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_error:
            error_message = "Error calling MReduce API: " + str(http_error)
            try:
                error_message += ".  Response payload:\n" + response.text
            except:
                self.logger.error("Error getting error response info", exc_info=True)
            self.logger.error(error_message)
            raise http_error
        return response

    def create_project(self, name):
        response = self.session.post(self.get_url("/api/v1/projects"), json={"name":name})
        response.raise_for_status()
        return response.json()

    def submit_job(self, projectId=None,  mapFunctionName=None, reduceFunctionName=None, finalizeFunctionName=None,
                   inputDatabase=None, inputCollection=None, queue=None, filter=None, outputCollection=None,
                   outputDatabase=None):
        if type(projectId) != str or not projectId:
            raise ValueError("Must supply projectId argument, type string")
        if type(mapFunctionName) != str or not mapFunctionName:
            raise ValueError("Must supply functionName argument, type string")
        if type(inputDatabase) != str or not inputDatabase:
            raise ValueError("Must supply inputDatabase argument, type string")
        if type(inputCollection) != str or not inputCollection:
            raise ValueError("Must supply collection argument, type string")
        url = self.get_url("/api/v1/projects/{projectId}/jobs".format(projectId=projectId))
        request_payload = {
            "inputDatabase": inputDatabase,
            "inputCollection": inputCollection,
            "mapFunctionName": mapFunctionName
        }

        if queue:
            request_payload["queue"] = queue
        if filter:
            request_payload["filter"] = filter
        if outputCollection:
            if not outputDatabase:
                raise ValueError("If setting outputCollection, must also set outputDatabase")
            request_payload["outputDatabase"] = outputDatabase
            request_payload["outputCollection"] = outputCollection
        if reduceFunctionName:
            request_payload["reduceFunctionName"] = reduceFunctionName
        if finalizeFunctionName:
            request_payload["finalizeFunctionName"] = finalizeFunctionName
        response = self.api_call("post", url, json=request_payload)
        return response.json()["job"]

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
        response = self.api_call("post", url, json=request_payload, timeout=timeout)
        response_payload = response.json()
        return response_payload["jobs"]

    def get_job(self, projectId, jobId):
        url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}".format(
                projectId=projectId, jobId=jobId
            )
        )
        response = self.api_call("get", url)
        return response.json()["job"]

    def initialize(self, projectId, job, workerId):
        self.logger.info("Initializing")
        stageIndex = job["currentStageIndex"]
        init_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageIndex}/initialize".format(
                projectId = projectId, jobId=job["_id"], stageIndex=stageIndex
            )
        )
        init_post_response = self.api_call("post", init_url, json={"workerId":workerId})
        if init_post_response.status_code == 204:
            return
        init_post_response_body = init_post_response.json()

        stage = init_post_response_body["job"]["stages"][stageIndex]
        collection_namespace = "{0}.{1}".format(stage["inputDatabase"], stage["inputCollection"])
        collections_bson = list(self.mongo_client.config.collections.find_raw_batches({"_id":collection_namespace}))
        init_query = None
        if collections_bson[0]:
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
            sort = [("_id", 1)]
        major_version, minor_version, patch_version = pymongo.version_tuple
        if (major_version == 3 and minor_version >= 7) or major_version > 3:
            count = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].estimated_document_count()
        else:
            count = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].count()
        chunks = 100
        skip = math.ceil(count / chunks)
        objectIdKeys = set()
        notObjectIdKeys = set()
        range_docs = []
        initialize_timeout = job["initializeTimeout"]
        update_time = int(time.time()) + (initialize_timeout / 2)
        collection = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]]
        for x in range(1,chunks+1):
            return_docs = list(collection.find(filter=init_query).sort(sort).skip(skip*x).limit(1))
            if return_docs:
                range_doc = {}
                for key in sort_keys:
                    value = return_docs[0][key]
                    if isinstance(value, bson.objectid.ObjectId):
                        if key in notObjectIdKeys:
                            raise ValueError(
                                "Mixed type for field {key}, cannot mix ObjectId and another type in the same field".format(
                                    key=key
                                )
                            )
                        objectIdKeys.add(key)
                        value = str(value)
                    elif key in objectIdKeys:
                        raise ValueError(
                            "Mixed type for field {key}, cannot mix ObjectId and another type in the same field".format(
                                key=key
                            )
                        )
                    else:
                        notObjectIdKeys.add(key)
                    range_doc[key] = value
                if range_docs:
                    if range_docs[-1] != range_doc:
                        range_docs.append(range_doc)
                else:
                    range_docs.append(range_doc)
            else:
                break
            if int(time.time()) >= update_time:
                self.api_call("patch", init_url, json={"workerId":workerId})
                update_time = int(time.time()) + (initialize_timeout / 2)
        init_put_payload = {
            "ranges": range_docs,
            "objectIdKeys":list(objectIdKeys)
        }
        self.api_call("put", init_url, json=init_put_payload)

    def cleanup(self, projectId, job):
        map_stage = job["stages"][0]
        self.mongo_client[map_stage["outputDatabase"]][job["tempCollectionName"]].drop()
        cleanup_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/cleanup".format(
                projectId=projectId, jobId=job["_id"]
            )
        )
        self.api_call("post", cleanup_url)

    def run(self, projectId, worker_functions, queue=None, batch_size=100):
        self.continue_working = True
        workerId = str(bson.ObjectId())
        while self.continue_working:
            work_url = self.get_url(
                "/api/v1/projects/{projectId}/work".format(
                    projectId=projectId
                )
            )
            params = {}
            if queue:
                params["queue"] = queue
            work_get_response = self.api_call("get", work_url, params=params)
            while work_get_response.status_code == 204:
                if not self.continue_working:
                    return
                time.sleep(10)
                work_get_response = self.api_call("get", work_url, params=params)
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
            post_response = self.api_call("post", range_url, json={"workerId": workerId})
            if post_response.status_code == 204:
                continue
            stage = job["stages"][job["currentStageIndex"]]
            filter = {}
            sort = None
            if work_get_payload["action"] == "map":
                filter = job.get("filter", {})
                sort = job.get("sort")
            elif work_get_payload["action"] == "reduce":
                map_ranges_url = "/api/v1/projects/{projectId}/jobs/{jobId}/stages/0/ranges".format(
                    projectId=projectId, jobId=job["_id"]
                )
                map_ranges_response = self.api_call("get", map_ranges_url)
                map_ranges_payload = map_ranges_response.json()
                valid_result_ids = [map_range["resultId"] for map_range in map_ranges_payload["ranges"]]
                filter = {"resultId":{"$in":valid_result_ids}}
                sort = [("key", pymongo.ASCENDING)]
            rangeStart = work_get_payload.get("rangeStart")
            rangeEnd = work_get_payload["rangeEnd"]
            filter.setdefault("$and", [])
            objectIdKeys = work_get_payload.get("objectIdKeys", [])
            for range_key in rangeStart.keys():
                startValue = rangeStart[range_key]
                if range_key in objectIdKeys:
                    startValue = bson.ObjectId(startValue)
                filter["$and"].append({range_key:{"$gte":startValue}})
                if rangeEnd:
                    endValue = rangeEnd[range_key]
                    if range_key in objectIdKeys:
                        endValue = bson.ObjectId(endValue)
                    filter["$and"].append({range_key:{"$lt":endValue}})
            print(sort)
            cursor = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].find(
                filter,
                batch_size=batch_size,
                sort=sort
            )
            do_work_function = worker_functions[stage["functionName"]]
            continue_working = True
            reduce_key = None
            reduce_values = []
            resultId = str(bson.ObjectId)
            while continue_working:
                if not self.continue_working:
                    return
                if int(time.time()) > update_time:
                    patch_response = self.api_call("patch", range_url, json={"workerId": workerId})
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
            self.api_call("put", range_url, json={"resultId": resultId})

    def stop(self):
        self.continue_working = False
