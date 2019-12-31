import collections
import logging
import math
import time
import traceback

import bson
import pymongo
import pymongo.errors
import requests
import requests.exceptions


class API:
    def __init__(self, api_key=None, mongo_client=None, logger=None, _host="mreduce.com"):
        """Create an instance of the MReduce API

        Args:
            api_key (str): MReduce API Key
            mongo_client: An instance of :py:class:`pymongo.mongo_client.MongoClient`
            logger: An instance of :py:class:`logging.Logger`
        """
        if type(api_key) != str:
            raise ValueError("Must supply api_key, type str")
        if not isinstance(mongo_client, pymongo.MongoClient):
            raise ValueError("Must supply mongo_client, type pymongo.MongoClient")
        self._host = _host
        self.session = self.get_session(api_key)
        self.mongo_client = mongo_client
        self.worker_functions = None
        if logger is None:
            logger = logging.getLogger(__name__)
        self.logger = logger

    def get_session(self, api_key, timeout=(10,10)):
        """Get an instance of :py:class:`requests.Session` for use with the MReduce API

        Args:
            api_key (str): MReduce API Key
            timeout (tuple): Same as the timeout argument for :py:func:`requests.request`
        """
        session = requests.Session()
        session.timeout = timeout
        session.headers.update({'x-api-key': api_key})
        return session

    def get_url(self, path):
        """Get a URL for the MReduce API given a requests path

        Args:
            path (str): The path of the URL
        """
        return "https://{0}{1}".format(self._host, path)

    def api_call(self, method, *args, **kwargs):
        """Make a request to the MReduce api using self.session

        Args:
            method (str): Same as the method argument for :py:func:`requests.request`
            *args: Extra arguments to pass to :py:func:`requests.request`
            **kwargs: Extra keyword arguments to pass to :py:func:`requests.request`

        Returns: py:class:`~mreduce.Job`
        """
        method = method.upper()
        response = self.session.request(method, *args, **kwargs)
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

    def submit_job(self, projectId=None,  mapFunctionName=None, reduceFunctionName=None, finalizeFunctionName=None,
                   inputDatabase=None, inputCollection=None, outputCollection=None,
                   outputDatabase=None, queue=None, filter=None):
        """Start a new job

        Args:
            projectId (str): The project ID from the MReduce dashboard
            mapFunctionName (str): Key to the map function in worker_functions :py:func:`mreduce.API.run`
            reduceFunctionName (str): Key to the reduce function in worker_functions :py:func:`mreduce.API.run`
            finalizeFunctionName (str): Key to the map function in worker_functions :py:func:`mreduce.API.run`
            inputDatabase (str): Name of the database where the inputCollection is
            inputCollection (str): Name of the collection to use as inputs to the map function
            outputDatabase (str): Name of the database where the outputCollection is
            outputCollection (str): Name of the collection to store output in
            queue (str): Submit this job to a queue.  Workers specify the queue they listen to in :py:func:`mreduce.API.run`
            filter (dict): MongoDB filter to match input documents
        """
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
        return Job(job_data, self)

    def list_jobs(self, projectId=None, filter=None, page=None, perPage=None, timeout=(10,10)):
        """List jobs

        Args:
            projectId: Project ID from the MReduce dashboard
            filter: MongoDB filter to filter jobs by
            page: Page number for result pagination
            perPage: Number of documents to return per page

        Returns: list of :py:class:`~mreduce.Job` instances
        """
        if type(projectId) != str or not projectId:
            raise ValueError("Must supply projectId type str")
        request_payload = {}
        if filter is not None:
            request_payload["filter"] = filter

        if page is not None:
            request_payload["page"] = page

        if perPage is not None:
            request_payload["perPage"] = perPage

        url = self.get_url("/api/v1/projects/{projectId}/jobs_search".format(projectId=projectId))
        response = self.api_call("post", url, json=request_payload, timeout=timeout)
        response_payload = response.json()
        jobs = []
        for job_data in response_payload["jobs"]:
            jobs.append(Job(job_data, self))
        return jobs

    def get_job(self, projectId, jobId):
        """Get a single job by ID

        Args:
            projectId: Project ID from the MReduce dashboard
            jobId: ID of the job to get

        Returns: :py:class:`~mreduce.Job`
        """
        url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}".format(
                projectId=projectId, jobId=jobId
            )
        )
        response = self.api_call("get", url)
        job_data = response.json()["job"]
        return Job(job_data, self)

    def _error_is_sharding_not_enabled(self, error):
        error_message = str(error)
        if not isinstance(error, pymongo.errors.OperationFailure):
            return False
        if "not found" in error_message or "sharding not enabled" in error_message:
            return True
        return False

    def _shard_output(self, output_namespace):
        try:
            self.mongo_client.admin.command('shardCollection', output_namespace, key={'_id': 1})
        except pymongo.errors.OperationFailure as operation_failure:
            if not self._error_is_sharding_not_enabled(operation_failure):
                raise operation_failure

    def run(self, projectId, worker_functions, queue=None):
        self.worker_functions = worker_functions
        self.continue_working = True
        self.last_ping_epoch = 0
        workerId = str(bson.ObjectId())
        while self.continue_working:
            work_url = self.get_url(
                "/api/v1/projects/{projectId}/work".format(
                    projectId=projectId
                )
            )
            work_request_body = {"workerId":workerId}
            if queue:
                work_request_body["queue"] = queue
            work_get_response = self.api_call("post", work_url, json=work_request_body)
            while work_get_response.status_code == 204:
                if not self.continue_working:
                    return
                time.sleep(1)
                work_get_response = self.api_call("post", work_url, json=work_request_body)
            work_get_payload = work_get_response.json()
            job = work_get_payload["job"]
            try:
                self._process_work(projectId, job, work_get_payload, workerId)
            except Exception as run_error:
                error_url = self.get_url(
                    "/api/v1/projects/{projectId}/jobs/{jobId}/error".format(
                        projectId=projectId, jobId=job["_id"]
                    )
                )
                error_payload = {
                    "traceback": traceback.format_exc(),
                    "message": "\n".join(traceback.format_exception_only(run_error.__class__, run_error)),
                    "stage": job["currentStage"]
                }
                self.api_call("post", error_url, json=error_payload)

    def _process_work(self, projectId, job, work_get_payload, workerId):
            if work_get_payload["action"] == "initialize":
                self._initialize(work_get_payload, job, workerId)
                return
            elif work_get_payload["action"] == "cleanup":
                self._cleanup(work_get_payload, job, workerId)
                return
            rangeIndex = work_get_payload["rangeIndex"]
            range_url = self.get_url(
                "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageName}/ranges/{rangeIndex}".format(
                    projectId=projectId,
                    jobId=job["_id"],
                    stageName=job["currentStage"],
                    rangeIndex=rangeIndex
                )
            )
            resultId = str(bson.ObjectId())
            if work_get_payload["action"] == "map":
                self._map(work_get_payload, job, resultId, workerId)
            elif work_get_payload["action"] == "reduce":
                self._reduce(work_get_payload, job, resultId, workerId)
            self.api_call("post", range_url, json={"resultId": resultId, "messageId":work_get_payload["messageId"]})

    def _initialize(self, work_get_payload, job, workerId):
        projectId = job["projectId"]
        self.logger.info("Initializing")
        stageName = job["currentStage"]
        init_query = None
        if stageName == "map":
            inputDatabase = job["inputDatabase"]
            inputCollection = job["inputCollection"]
            input_namespace = "{0}.{1}".format(inputDatabase, inputCollection)
            if "reduceFunctionName" in job:
                outputDatabase = job["outputDatabase"]
                outputCollection = job["tempCollection"]
                output_namespace = "{0}.{1}".format(outputDatabase, outputCollection)
                self._shard_output(output_namespace)
            collections_bson = list(self.mongo_client.config.collections.find_raw_batches({"_id": input_namespace}))
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
        elif stageName == "reduce":
            inputDatabase = job["tempDatabase"]
            inputCollection = job["tempCollection"]
            outputDatabase = job["outputDatabase"]
            outputCollection = job["outputCollection"]
            output_namespace = "{0}.{1}".format(outputDatabase, outputCollection)
            self.mongo_client[outputDatabase][outputCollection].drop()
            self._shard_output(output_namespace)
            sort_keys = ["key"]
            sort = [("key", 1)]
        else:
            raise ValueError("Invalid stage name: " + stageName)

        major_version, minor_version, patch_version = pymongo.version_tuple
        if (major_version == 3 and minor_version >= 7) or major_version > 3:
            count = self.mongo_client[inputDatabase][inputCollection].estimated_document_count()
        else:
            count = self.mongo_client[inputDatabase][inputCollection].count()
        chunks = job["numRanges"]
        skip = math.ceil(count / chunks)
        objectIdKeys = set()
        notObjectIdKeys = set()
        range_docs = []
        lastPing = int(time.time())
        collection = self.mongo_client[inputDatabase][inputCollection]
        for x in range(0,chunks):
            if self._due_for_ping(lastPing, job["initializeTimeout"]):
                lastPing = int(time.time())
                ping_response = self._ping_message(projectId, job["_id"], work_get_payload["messageId"], workerId)
                if ping_response.status_code == 204:
                    return
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


        init_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/stages/{stageName}/initialize".format(
                projectId = projectId, jobId=job["_id"], stageName=stageName
            )
        )
        init_put_payload = {
            "ranges": range_docs,
            "objectIdKeys":list(objectIdKeys),
            "messageId": work_get_payload["messageId"]
        }
        self.api_call("post", init_url, json=init_put_payload)

    def _cleanup(self, work_get_payload, job, workerId):
        projectId = job["projectId"]
        self.mongo_client[job["tempDatabase"]][job["tempCollection"]].drop()
        if "reduceFunctionName" in job:
            range_url = self.get_url(
                "/api/v1/projects/{projectId}/jobs/{jobId}/stages/reduce/ranges".format(
                    projectId=projectId, jobId=job["_id"]
                )
            )
            ranges = []
            try:
                range_response = self.api_call("get", range_url)
                range_payload = range_response.json()
                ranges = range_payload["ranges"]
            except requests.exceptions.HTTPError as http_error:
                if http_error.response.status_code != 404:
                    raise http_error

            result_ids = []
            for range in ranges:
                if "resultId" in range:
                    result_ids.append(range["resultId"])
            self.mongo_client[job["outputDatabase"]][job["outputCollection"]].delete_many({
                "resultId":{"$not":{"$in":result_ids}}
            })
        cleanup_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/cleanup".format(
                projectId=projectId, jobId=job["_id"]
            )
        )
        cleanup_request_body = {"messageId":work_get_payload["messageId"]}
        self.api_call("post", cleanup_url, json=cleanup_request_body)

    def _due_for_ping(self, lastPingEpoch, timeout):
        update_time = lastPingEpoch + (timeout / 2)
        current_time = int(time.time())
        if current_time >= update_time:
            return True
        return False

    def _ping_message(self, projectId, jobId, messageId, workerId):
        ping_url = self.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}/messages/{messageId}".format(
                projectId=projectId,
                jobId=jobId,
                messageId=messageId
            )
        )
        ping_response = self.api_call("post", ping_url, json={"workerId": workerId})
        return ping_response

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

    def _map(self, work_get_payload, job, resultId, workerId, batch_size=100):
        projectId = job["projectId"]
        rangeStart = work_get_payload["rangeStart"]
        rangeEnd = work_get_payload.get("rangeEnd")
        objectIdKeys = work_get_payload.get("objectIdKeys", [])
        range_filter = self._build_range_filter(rangeStart, rangeEnd=rangeEnd, objectIdKeys=objectIdKeys)
        query = job.get("filter", {})
        query.setdefault("$and", [])
        query["$and"] += range_filter
        sort = job.get("sort")
        cursor = self.mongo_client[job["inputDatabase"]][job["inputCollection"]].find(
            query,
            batch_size=batch_size,
            sort=sort
        )
        map_function_name = job["mapFunctionName"]
        map_function = self.worker_functions[map_function_name]
        documents = self._get_batch(cursor, batch_size)
        lastPing = int(time.time())
        while documents:
            if self._due_for_ping(lastPing, job["workTimeout"]):
                lastPing = int(time.time())
                ping_response = self._ping_message(projectId, job["_id"], work_get_payload["messageId"], workerId)
                if ping_response.status_code == 204:
                    return
            insert_docs = []
            mapped_values = {}
            for doc in documents:
                if not self.continue_working:
                    return
                for key, value in map_function(doc):
                    mapped_values.setdefault(key, [])
                    mapped_values[key].append(value)
            for key in mapped_values.keys():
                values = mapped_values[key]
                insert_docs.append({"key": key, "values": values})
            if insert_docs:
                for doc in insert_docs:
                    doc["resultId"] = resultId
                self.mongo_client[job["tempDatabase"]][job["tempCollection"]].insert_many(insert_docs)
            documents = self._get_batch(cursor, batch_size)

    def _reduce(self, work_get_payload, job, resultId, workerId):
        projectId = job["projectId"]
        map_ranges_url = self.get_url("/api/v1/projects/{projectId}/jobs/{jobId}/stages/map/ranges".format(
            projectId=job["projectId"], jobId=job["_id"]
        ))
        map_ranges_response = self.api_call("get", map_ranges_url)
        map_ranges_payload = map_ranges_response.json()
        reduce_function_name = job["reduceFunctionName"]
        reduce_function = self.worker_functions[reduce_function_name]
        valid_result_ids = [map_range["resultId"] for map_range in map_ranges_payload["ranges"]]
        rangeStart = work_get_payload["rangeStart"]
        rangeEnd = work_get_payload.get("rangeEnd")
        range_filter = self._build_range_filter(rangeStart, rangeEnd=rangeEnd)
        finalize_function = None
        finalize_function_name = job.get("finalizeFunctionName")
        if finalize_function_name:
            finalize_function = self.worker_functions[finalize_function_name]
        query = {
            "resultId": {"$in": valid_result_ids},
            "$and": range_filter
        }
        cursor = self.mongo_client[job["tempDatabase"]][job["tempCollection"]].find(
            query,
            sort=[("key", pymongo.ASCENDING)]
        )
        previous_key = None
        values = []
        lastPing = int(time.time())
        doc_count = 0
        for document in cursor:
            doc_count += 1
            if doc_count >= 100:
                if self._due_for_ping(lastPing, job["workTimeout"]):
                    lastPing = int(time.time())
                    ping_response = self._ping_message(projectId, job["_id"], work_get_payload["messageId"], workerId)
                    if ping_response.status_code == 204:
                        return
                doc_count = 0
            key = document["key"]
            if previous_key != key:
                if len(values) > 0:
                    if len(values) > 1:
                        value = reduce_function(previous_key, values)
                    else:
                        value = values[0]
                    if finalize_function:
                        value = finalize_function(previous_key, value)
                    self.mongo_client[job["outputDatabase"]][job["outputCollection"]].insert_one(
                        {
                            "_id": previous_key,
                            "value":value,
                            "resultId": resultId
                        }
                    )
                    values = []
            else:
                if len(values) > 10:
                    values = [reduce_function(key, values)]
            values += document["values"]
            previous_key = key
        if len(values) > 1:
            value = reduce_function(previous_key, values)
        else:
            value = values[0]
        if finalize_function:
            value = finalize_function(previous_key, value)
        self.mongo_client[job["outputDatabase"]][job["outputCollection"]].insert_one(
            {
                "_id": previous_key,
                "value": value,
                "resultId": resultId
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

class Job(collections.UserDict):
    def __init__(self, data, api):
        """Represents a Job in the MReduce API

        Generally you will not create instances of this class yourself
        """
        super(Job, self).__init__(data)
        self.api = api

    def wait_for_result(self, timeout=None):
        """Wait for the job to complete

        Args:
            timeout (int): Only wait up to this many seconds
        """
        job_url = self.api.get_url(
            "/api/v1/projects/{projectId}/jobs/{jobId}".format(
                projectId = self["projectId"],
                jobId = self["_id"]
            )
        )
        start_time = int(time.time())
        while self["status"] in ["running", "cleanup"]:
            if timeout:
                if int(time.time()) > start_time + timeout:
                    break
            job_response = self.api.api_call("get", job_url)
            job_payload = job_response.json()
            self.data = job_payload["job"]
            time.sleep(1)
        if self["status"] in ["running", "cleanup"]:
            raise TimeoutError("Timed out waiting for job to complete")
        result = self.get_result()
        return result

    def get_result(self):
        """Get the result of a job

        Returns a generator of (key, value) pairs.  Will raise an error if the job is not complete yet or has errors
        """
        if self["status"] == "running":
            raise JobRunningError("Cannot get result until job is complete.  See wait_for_result")
        elif self["status"] == "error":
            error_info = self["errorInfo"]
            raise JobNotCompleteError("Job has errors: \n" + error_info["traceback"])
        elif self["status"] != "complete":
            raise JobNotCompleteError("Job not complete.  Status: " + self["status"])
        elif "reduceFunctionName" not in self:
            raise ValueError("Cannot get result for a job which does not specify a reduce function")
        cursor = self.api.mongo_client[self["outputDatabase"]][self["outputCollection"]].find()
        for document in cursor:
            yield document["_id"], document["value"]
