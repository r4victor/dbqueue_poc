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
* A Heartbeater tracks all rows in the pipeline (in the queue or in processing), and updates the lock expiration. This allows setting small `lock_expires_at` and pick up stale rows quickly.
* A fetcher performs the fetch when the queue size goes under a configured lower limit. It has exponential retry delays between empty fetches.
* There is a fetch hint mechanism that services can use to notify the pipelines within the replica – in that case the fetcher stops sleeping and fetches immediately.

## Project structure

* `dbqueue_poc/`
  * `background/`
    * `pipeline_tasks/` – tasks running continuously via pipelines
      * `process_jobs.py` - an example of a pipeline processing one resource type that is also process by another pipeline
      * `process_placement_group.py` – an example of the simplest pipeline processing one resource type
      * `process_runs.py` – an example of a pipeline processing a resource type and related resource types
    * `scheduled_tasks/` – tasks running periodically via scheduler
