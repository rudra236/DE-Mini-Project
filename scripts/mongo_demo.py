"""BankFlow Sentinel - MongoDB CRUD & Operations Demo (Experiment 4).

Demonstrates and prints labelled output for:
1. create_index (and show index_information)
2. insert_one / insert_many
3. find with filter
4. projection
5. update_one / update_many
6. delete_one
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pymongo.errors import DuplicateKeyError

from db.mongo_setup import get_mongo_db


def run_mongo_demo():
    print("=" * 70)
    print(" BankFlow Sentinel - MongoDB CRUD Operations Demo (Experiment 4)")
    print("=" * 70)

    db = get_mongo_db("bankflow_demo")
    raw_col = db["raw_events"]
    source_col = db["source_metadata"]

    # Clean demo collections for clean run
    raw_col.drop()
    source_col.drop()

    # -------------------------------------------------------------------------
    # STEP 1: create_index and index_information
    # -------------------------------------------------------------------------
    print("\n[STEP 1] CREATE_INDEX & INDEX_INFORMATION")
    print("-" * 70)
    # Unique index on source_metadata.source_id
    idx_source = source_col.create_index([("source_id", 1)], unique=True, name="idx_source_id_unique")
    print(f"Created index on source_metadata: {idx_source}")
    print("Index Information (source_metadata):")
    pprint(source_col.index_information())

    # Index on raw_events.batch_id
    idx_raw = raw_col.create_index([("batch_id", 1)], name="idx_batch_id")
    print(f"\nCreated index on raw_events: {idx_raw}")
    print("Index Information (raw_events):")
    pprint(raw_col.index_information())

    # -------------------------------------------------------------------------
    # STEP 2: insert_one and insert_many
    # -------------------------------------------------------------------------
    print("\n[STEP 2] INSERT_ONE & INSERT_MANY")
    print("-" * 70)
    # insert_one into source_metadata
    src_doc1 = {
        "source_id": "SRC_CORE_BANKING",
        "name": "Core Banking Gateway",
        "type": "OLTP_RDBMS",
        "owner": "Payment Infrastructure Team",
        "schema_version": "1.0.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    res_one = source_col.insert_one(src_doc1)
    print(f"[insert_one] source_metadata inserted ID: {res_one.inserted_id}")

    # insert_many into raw_events
    events = [
        {
            "batch_id": "B001",
            "file_path": "data/batch_001",
            "row_count": 10000,
            "channel": "API_INGEST",
            "status": "pending",
            "received_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "batch_id": "B002",
            "file_path": "data/batch_002",
            "row_count": 100,
            "channel": "API_INGEST",
            "status": "pending",
            "received_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "batch_id": "B003",
            "file_path": "data/batch_003",
            "row_count": 500,
            "channel": "STREAM_KAFKA",
            "status": "pending",
            "received_at": datetime.now(timezone.utc).isoformat(),
        },
    ]
    res_many = raw_col.insert_many(events)
    print(f"[insert_many] raw_events inserted {len(res_many.inserted_ids)} documents.")

    # -------------------------------------------------------------------------
    # STEP 3: find with filter
    # -------------------------------------------------------------------------
    print("\n[STEP 3] FIND WITH FILTER")
    print("-" * 70)
    query_filter = {"batch_id": "B001"}
    print(f"Executing query filter: {query_filter}")
    found_event = raw_col.find_one(query_filter, {"_id": 0})
    print("Found Document:")
    pprint(found_event)

    # -------------------------------------------------------------------------
    # STEP 4: Projection
    # -------------------------------------------------------------------------
    print("\n[STEP 4] PROJECTION")
    print("-" * 70)
    projection = {"source_id": 1, "name": 1, "schema_version": 1, "_id": 0}
    print(f"Applying Projection: {projection}")
    projected = list(source_col.find({}, projection))
    print("Projected Results:")
    pprint(projected)

    # -------------------------------------------------------------------------
    # STEP 5: update_one and update_many
    # -------------------------------------------------------------------------
    print("\n[STEP 5] UPDATE_ONE & UPDATE_MANY")
    print("-" * 70)
    # update_one
    up_one_res = source_col.update_one(
        {"source_id": "SRC_CORE_BANKING"},
        {"$set": {"schema_version": "1.1.0", "updated_at": datetime.now(timezone.utc).isoformat()}},
    )
    print(f"[update_one] Matched: {up_one_res.matched_count}, Modified: {up_one_res.modified_count}")
    updated_source = source_col.find_one({"source_id": "SRC_CORE_BANKING"}, {"_id": 0})
    print("Updated Document:")
    pprint(updated_source)

    # update_many
    up_many_res = raw_col.update_many(
        {"status": "pending"},
        {"$set": {"status": "processed", "processed_at": datetime.now(timezone.utc).isoformat()}},
    )
    print(f"\n[update_many] Matched: {up_many_res.matched_count}, Modified: {up_many_res.modified_count}")

    # -------------------------------------------------------------------------
    # STEP 6: delete_one
    # -------------------------------------------------------------------------
    print("\n[STEP 6] DELETE_ONE")
    print("-" * 70)
    del_res = raw_col.delete_one({"batch_id": "B003"})
    print(f"[delete_one] Deleted {del_res.deleted_count} document with batch_id='B003'.")
    remaining_count = raw_col.count_documents({})
    print(f"Remaining documents in raw_events: {remaining_count}")

    # -------------------------------------------------------------------------
    # STEP 7: Unique Index Constraint Verification
    # -------------------------------------------------------------------------
    print("\n[STEP 7] UNIQUE INDEX ENFORCEMENT VERIFICATION")
    print("-" * 70)
    try:
        source_col.insert_one({"source_id": "SRC_CORE_BANKING", "name": "Duplicate Attempt"})
        print("ERROR: Duplicate insert should have failed!")
    except DuplicateKeyError as e:
        print("SUCCESS: Unique index prevented duplicate source_id:")
        print(f"  {e.details.get('errmsg', str(e))}")

    print("\n" + "=" * 70)
    print(" MONGODB CRUD DEMO COMPLETED SUCCESSFULLY (ALL STEPS PASS)")
    print("=" * 70)


if __name__ == "__main__":
    run_mongo_demo()
