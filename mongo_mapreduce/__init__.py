import collections
import logging
import math
import time
import traceback
import sys

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
        job_data = response.json()["job"]
        return MongoMapreduceJob(job_data, self)

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
        jobs = []
        for job_data in response_payload["jobs"]:
            jobs.append(MongoMapreduceJob(job_data, self))
        return jobs

    def get_job(self, projectId, jobId):
        url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}".format(
                projectId=projectId, jobId=jobId
            )
        )
        response = self.api_call("get", url)
        job_data = response.json()["job"]
        return MongoMapreduceJob(job_data, self)

    def _initialize(self, projectId, job):
        self.logger.info("Initializing")
        stageIndex = job["currentStageIndex"]
        init_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageIndex}/initialize".format(
                projectId = projectId, jobId=job["_id"], stageIndex=stageIndex
            )
        )
        init_post_response = self.api_call("post", init_url, json={"workerId":self.workerId})
        if init_post_response.status_code == 204:
            return
        init_post_response_body = init_post_response.json()
        job = init_post_response_body["job"]
        stage = job["stages"][stageIndex]
        self.mongo_client[stage["outputDatabase"]][stage["outputCollection"]].drop()
        collection_namespace = "{0}.{1}".format(stage["inputDatabase"], stage["inputCollection"])
        collections_bson = list(self.mongo_client.config.collections.find_raw_batches({"_id":collection_namespace}))
        init_query = None
        if stageIndex == 0:
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
        else:
            sort_keys = ["key"]
            sort = [("key", 1)]
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
        for x in range(0,chunks):
            if x < chunks:
                return_docs = list(collection.find(filter=init_query).sort(sort).skip(skip*x).limit(1))
            else:
                sort_backwards = [(key, value * -1) for key, value in sort]
                return_docs = list(collection.find(filter=init_query).sort(sort_backwards).limit(1))

            if return_docs:
                range_values_doc = {}
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
                    range_values_doc[key] = value
                if range_docs:
                    if range_docs[-1]["values"] != range_values_doc:
                        range_docs.append({"values": range_values_doc})
                else:
                    range_docs.append({"values": range_values_doc})
            else:
                break
            if int(time.time()) >= update_time:
                self.api_call("patch", init_url, json={"workerId":self.workerId})
                update_time = int(time.time()) + (initialize_timeout / 2)
        init_put_payload = {
            "ranges": range_docs,
            "objectIdKeys":list(objectIdKeys)
        }
        self.api_call("put", init_url, json=init_put_payload)

    def _cleanup(self, projectId, job):
        map_stage = job["stages"][0]
        self.mongo_client[map_stage["outputDatabase"]][job["tempCollectionName"]].drop()
        if "reduceFunctionName" in job:
            range_url = self.get_url(
                "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageIndex}/ranges".format(
                    projectId=projectId, jobId=job["_id"], stageIndex=1
                )
            )
            range_response = self.api_call("get", range_url)
            range_payload = range_response.json()
            result_ids = []
            for range in range_payload["ranges"]:
                result_ids.append(range["resultId"])
            reduce_stage = job["stages"][1]
            self.mongo_client[reduce_stage["outputDatabase"]][reduce_stage["outputCollection"]].delete_many({
                "resultId":{"$not":{"$in":result_ids}}
            })
        cleanup_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/cleanup".format(
                projectId=projectId, jobId=job["_id"]
            )
        )
        self.api_call("post", cleanup_url)

    def run(self, projectId, worker_functions, queue=None, batch_size=100):
        self.worker_functions = worker_functions
        self.continue_working = True
        self.last_ping_epoch = 0
        self.workerId = str(bson.ObjectId())
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
            try:
                self._process_work(projectId, job, work_get_payload)
            except Exception as run_error:
                error_url = self.get_url(
                    "/api/v1/projects/{projectId}/jobs/{jobId}/error".format(
                        projectId=projectId, jobId=job["_id"]
                    )
                )
                error_payload = {
                    "traceback": traceback.format_exc(),
                    "message": "\n".join(traceback.format_exception_only(run_error.__class__, run_error)),
                    "stageIndex": job["currentStageIndex"]
                }
                self.api_call("post", error_url, json=error_payload)

    def _process_work(self, projectId, job, work_get_payload):
            if work_get_payload["action"] == "initialize":
                self._initialize(projectId, job)
                return
            elif work_get_payload["action"] == "cleanup":
                self._cleanup(projectId, job)
                return
            rangeIndex = work_get_payload["rangeIndex"]
            range_url = self.get_url(
                "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageIndex}/ranges/{rangeIndex}".format(
                    projectId=projectId,
                    jobId=job["_id"],
                    stageIndex=job["currentStageIndex"],
                    rangeIndex=rangeIndex
                )
            )
            post_response = self.api_call("post", range_url, json={"workerId": self.workerId})
            if post_response.status_code == 204:
                return
            resultId = str(bson.ObjectId())
            if work_get_payload["action"] == "map":
                self._map(work_get_payload, job, resultId)
            elif work_get_payload["action"] == "reduce":
                self._reduce(work_get_payload, job, resultId)
            elif work_get_payload["action"] == "finalize":
                self._finalize(work_get_payload, job)
            self.api_call("put", range_url, json={"resultId": resultId})

    def _ping_range(self, job, rangeIndex):
        range_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageIndex}/ranges/{rangeIndex}".format(
                projectId=job["projectId"],
                jobId=job["_id"],
                stageIndex=job["currentStageIndex"],
                rangeIndex=rangeIndex
            )
        )
        update_time = self.last_ping_epoch + (job["workTimeout"] / 2)
        current_time = int(time.time())
        if current_time >= update_time:
            self.last_ping_epoch = current_time
            patch_response = self.api_call("patch", range_url, json={"workerId": self.workerId})
            return patch_response

    def _build_range_filter(self, rangeStart, rangeEnd=None, objectIdKeys=None):
        range_filter = []
        if objectIdKeys is None:
            objectIdKeys = set()
        for range_key in rangeStart.keys():
            startValue = rangeStart[range_key]
            if range_key in objectIdKeys:
                startValue = bson.ObjectId(startValue)
            range_filter.append({range_key: {"$gte": startValue}})
            if rangeEnd:
                endValue = rangeEnd[range_key]
                if range_key in objectIdKeys:
                    endValue = bson.ObjectId(endValue)
                range_filter.append({range_key: {"$lt": endValue}})
        return range_filter

    def _get_batch(self, cursor, batch_size):
        documents = []
        for x in range(0, batch_size):
            if not self.continue_working:
                return
            try:
                documents.append(cursor.next())
            except StopIteration:
                break
        return documents

    def _map(self, work_payload, job, resultId, batch_size=100):
        rangeStart = work_payload["rangeStart"]
        rangeEnd = work_payload.get("rangeEnd")
        objectIdKeys = work_payload.get("objectIdKeys", [])
        range_filter = self._build_range_filter(rangeStart, rangeEnd=rangeEnd, objectIdKeys=objectIdKeys)
        query = job.get("filter", {})
        query.setdefault("$and", [])
        query["$and"] += range_filter
        sort = job.get("sort")
        stage = job["stages"][0]
        cursor = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].find(
            query,
            batch_size=batch_size,
            sort=sort
        )
        map_function_name = stage["functionName"]
        map_function = self.worker_functions[map_function_name]
        documents = self._get_batch(cursor, batch_size)
        while documents:
            ping_response = self._ping_range(job, work_payload["rangeIndex"])
            if ping_response is not None:
                if ping_response.status_code == 204:
                    break
            insert_docs = []
            mapped_values = {}
            for doc in documents:
                if not self.continue_working:
                    return
                key, value = map_function(doc)
                mapped_values.setdefault(key, [])
                mapped_values[key].append(value)
            for key in mapped_values.keys():
                values = mapped_values[key]
                insert_docs.append({"key": key, "values": values})
            if insert_docs:
                for doc in insert_docs:
                    doc["resultId"] = resultId
                try:
                    self.mongo_client[stage["outputDatabase"]][stage["outputCollection"]].insert_many(insert_docs)
                except pymongo.errors.DuplicateKeyError as duplicate_key_error:
                    pass
            documents = self._get_batch(cursor, batch_size)

    def _reduce(self, work_payload, job, resultId):
        map_ranges_url = self.get_url("/api/v1/projects/{projectId}/jobs/{jobId}/stages/0/ranges".format(
            projectId=job["projectId"], jobId=job["_id"]
        ))
        map_ranges_response = self.api_call("get", map_ranges_url)
        map_ranges_payload = map_ranges_response.json()
        stage = job["stages"][1]
        reduce_function_name = stage["functionName"]
        reduce_function = self.worker_functions[reduce_function_name]
        valid_result_ids = [map_range["resultId"] for map_range in map_ranges_payload["ranges"]]
        rangeStart = work_payload["rangeStart"]
        rangeEnd = work_payload.get("rangeEnd")
        range_filter = self._build_range_filter(rangeStart, rangeEnd=rangeEnd)
        query = {
            "resultId": {"$in": valid_result_ids},
            "$and": range_filter
        }
        cursor = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].find(
            query,
            sort=[("key", pymongo.ASCENDING)]
        )
        previous_key = None
        values = []
        doc_count = 0
        for document in cursor:
            doc_count += 1
            if doc_count >= 100:
                ping_response = self._ping_range(job, work_payload["rangeIndex"])
                if ping_response is not None:
                    if ping_response.status_code == 204:
                        break
                doc_count = 0
            key = document["key"]
            if previous_key != key:
                if len(values) > 0:
                    if len(values) > 1:
                        value = reduce_function(values)
                    else:
                        value = values[0]
                    self.mongo_client[stage["outputDatabase"]][stage["outputCollection"]].insert_one(
                        {
                            "_id": previous_key,
                            "value":value,
                            "resultId": resultId
                        }
                    )
                    values = []
            else:
                if len(values) > 10:
                    values = [reduce_function(values)]
            values += document["values"]
            previous_key = key
        if len(values) > 1:
            value = reduce_function(values)
        else:
            value = values[0]
        self.mongo_client[stage["outputDatabase"]][stage["outputCollection"]].insert_one(
            {
                "_id": previous_key,
                "value": value,
                "resultId": resultId
            }
        )


    def _finalize(self, work_payload, job):
        stage = job["stages"][2]
        finalize_function_name = stage["functionName"]
        finalize_function = self.worker_functions[finalize_function_name]
        rangeStart = work_payload["rangeStart"]
        rangeEnd = work_payload.get("rangeEnd")
        range_filter = self._build_range_filter(rangeStart, rangeEnd=rangeEnd)
        query = {"$and":range_filter}
        cursor = self.mongo_client[stage["inputDatabase"]][stage["inputCollection"]].find(
            query
        )
        doc_count = 0
        for document in cursor:
            doc_count += 1
            if doc_count >= 100:
                ping_response = self._ping_range(job, work_payload["rangeIndex"])
                if ping_response is not None:
                    if ping_response.status_code == 204:
                        break
            if document.get("finalized"):
                continue
            finalized_value = finalize_function(document["_id"], document["value"])
            self.mongo_client[stage["outputDatabase"]][stage["outputCollection"]].find_one_and_update(
                {
                    "_id": document["_id"],
                    "finalized": {"$exists": False}
                },
                {
                    "$set": {
                        "value": finalized_value,
                        "finalized": True
                    }
                }
            )

    def stop(self):
        self.continue_working = False

class MongoMapreduceError(Exception):
    pass

class JobNotCompleteError(MongoMapreduceError):
    pass

class JobRunningError(MongoMapreduceError):
    pass

class TimeoutError(MongoMapreduceError):
    pass

class MongoMapreduceJob(collections.UserDict):
    def __init__(self, data, api):
        super(MongoMapreduceJob, self).__init__(data)
        self.api = api

    def wait_for_result(self, timeout=None):
        job_url = self.api.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}".format(
                projectId = self["projectId"],
                jobId = self["_id"]
            )
        )
        start_time = int(time.time())
        while self["running"]:
            if timeout:
                if int(time.time()) > start_time + timeout:
                    break
            job_response = self.api.api_call("get", job_url)
            job_payload = job_response.json()
            self.data = job_payload["job"]
            time.sleep(10)
        if self["running"]:
            raise TimeoutError("Timed out waiting for job to complete")
        if not self["complete"]:
            raise JobNotCompleteError("Job has errors")
        result = self.get_result()
        return result

    def get_result(self):
        if not self["complete"]:
            raise JobRunningError("Cannot get result until job is complete.  See wait_for_result")
        if len(self["stages"]) < 2:
            raise ValueError("Cannot get result for a job which does not specify a reduce function")
        reduce_stage = self["stages"][1]
        cursor = self.api.mongo_client[reduce_stage["outputDatabase"]][reduce_stage["outputCollection"]].find()
        for document in cursor:
            yield document["_id"], document["value"]