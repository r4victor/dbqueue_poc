# dbqueue

## Overview

dbqueue is a proof of concept implementation of efficient background processing in a FastAPI app
build on top of database queues using SELECT FOR UPDATE SKIP LOCKED on Postgres and in-memory locks on SQLite.
The same ides are proposed to be implemented in [`dstack`](https://github.com/dstackai/dstack).

## Implementation details

* Resources are continuously processed in the background by pipelines. A pipeline consists of a fetcher, workers, and a heartbeater.
* A fetcher selects rows to be processed from the DB, marks them as locked in the DB, and puts them into an in-memory queue. 
* Workers consume rows from the in-memory queue, process the rows, and unlock them.
* The locking (unlocking) is done by setting (unsetting) `lock_expires_at`, `lock_token`, `lock_owner`.
* If the replica/pipeline dies, the rows stay locked in the db. Another replica picks up the rows after `lock_expires_at`.
* `lock_token` prevents stale replica/pipeline to update the rows already picked up by the new replica.
* `lock_owner` stores the pipeline that's locked the row so that only that pipeline can recover if it's stale.
* A heartbeater tracks all rows in the pipeline (in the queue or in processing), and updates the lock expiration. This allows setting small `lock_expires_at` and picking up stale rows quickly
* A fetcher performs the fetch when the queue size goes under a configured lower limit. It has exponential retry delays between empty fetches, thus reducing load on the DB.
* There is a fetch hint mechanism that services can use to notify the pipelines within the replica – in that case the fetcher stops sleeping and fetches immediately.

## Performance analysis

* Pipeline throughput = workers_num / worker_processing_time. So quick tasks easily give high-throughput pipelines, e.g. 1s task with 20 workers is 1200 tasks/min.
A slow 30s task gives only 40 tasks/min with the same number of workers. We can increase the number of workers but the peak memory usage will grow proportionally.
In general, workers should be optimized to be as quick as possible to improve throughput.
* Processing latency (wait) is close to 0 due to fetch hints if the pipeline is not saturated. In general, latency = queue_size / throughput.
* In-memory queue maxsize provides a cap on memory usage and recovery time after crashes (number of locked items to retry).
* Fetcher's DB load is proportional to the number of pipelines and is expected to be negligible. Workers can put a considerable read/write DB load as it's proportional to the number of workers. This can be optimized by batching workers' writes. Workers do processing outside of transactions so DB connections won't be a bottleneck.
* There is a risk of lock starvation if a worker needs to lock all related resources. This is to be mitigated by 1) related pipelines checking `lock_owner` and skip locking to let the parent pipeline acquire all the locks eventually and 2) do the related resource locking only on paths that require it.

## Project structure

* `dbqueue_poc/`
  * `background/`
    * `pipeline_tasks/` – tasks running continuously via pipelines
      * `process_jobs.py` - a pipeline processing one resource type that is also process by another pipeline
      * `process_placement_group.py` – a simple pipeline processing one resource type
      * `process_runs.py` – a pipeline processing a resource type and related resource types
    * `scheduled_tasks/` – tasks running periodically via scheduler

## TBD

* Consider separating writers from workers to batch the updates to reduce the write load on the DB.
