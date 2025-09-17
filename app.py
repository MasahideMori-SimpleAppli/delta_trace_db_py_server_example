from delta_trace_db.db.delta_trace_db_core import DeltaTraceDatabase
from delta_trace_db.query.cause.permission import Permission
from delta_trace_db.query.enum_query_type import EnumQueryType
from delta_trace_db.query.query import Query
from delta_trace_db.query.transaction_query import TransactionQuery
from delta_trace_db.query.util_query import UtilQuery
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
import os
from datetime import datetime, timezone
import uuid
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import logging

# ---------------------------
# Create scheduler
# ---------------------------
scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan_context(fastapi_app: FastAPI):
    # Startup processing
    # Enter this here if you want to perform DB restore(from_dict) processing.
    if not scheduler.running:
        scheduler.add_job(_backup_db, 'cron', hour=1, minute=0, id="daily_backup")
        # TODO During testing, enabling this allows saving every 10 seconds.
        # scheduler.add_job(_backup_db, 'interval', seconds=10, id="test_backup", replace_existing=True)
        scheduler.start()
    yield
    # Shutdown processing
    scheduler.shutdown()


# ---------------------------
# Create FastAPI app
# ---------------------------
app = FastAPI(lifespan=lifespan_context)

# CORS settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (for development)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# In-memory DB (global variable)
# ---------------------------
delta_trace_db = DeltaTraceDatabase()


# ---------------------------
# Endpoints
# ---------------------------
@app.post("/backend")
async def backend_db(request: Request):
    # TODO First, verify the JWT to check if the user is valid.

    # If verification is fine, extract the query JSON.
    # Here, we temporarily restore it to the query class for convenience.
    query_json = await request.json()
    query = UtilQuery.convert_from_json(query_json)
    # TODO Restrict executable queries according to the permissions of the operating user.
    # TODO Note that if you only write allowed ones, collection_permissions=None (default) allows all for frontend only.
    # TODO The key is the collection name. Only access to collections with specified permissions is allowed; others are denied.
    # TODO For example, with the following permissions, access to "users" with add, getAll or search is allowed, but clear cannot be used, and access to collections like "users2" is denied.
    collection_permissions = {"users": Permission([EnumQueryType.add, EnumQueryType.getAll, EnumQueryType.search])}
    result = delta_trace_db.execute_query_object(query=query, collection_permissions=collection_permissions)
    # Only if successful, save the query as a log.
    if result.is_success:
        # TODO If encryption is needed, adjust inside save_json_file function.
        _save_success_query(query=query_json)
    else:
        # For cases such as forbidden calls,
        # Failed cases can be returned as result with 200 OK (frontend sees isSuccess=false),
        # or handled individually and returned as error as below.
        # Save error queries for investigation.
        _save_error_query(query=query_json)
        if isinstance(query, Query):
            if not UtilQuery.check_permissions(query, collection_permissions=collection_permissions):
                raise HTTPException(
                    status_code=403,
                    detail="Operation not permitted."
                )
        elif isinstance(query, TransactionQuery):
            for q in query.queries:
                if not UtilQuery.check_permissions(q, collection_permissions=collection_permissions):
                    raise HTTPException(
                        status_code=403,
                        detail="Operation not permitted."
                    )
        else:
            raise HTTPException(
                status_code=400,
                detail="Processing failed."
            )
    return result.to_dict()


# ---------------------------
# Auto backup, log saving, etc.
# ---------------------------
def _save_log(log:str):
    # クエリ以外のエラーログなど（OSの種類やログの頻度によっては制限を調整してください）
    return save_json_file({"log" : log}, folder="logs", prefix="log", max_files=100000, exp=".log")


def _save_success_query(query):
    # Query log (Save without limit. Please limit storage depending on the OS type and log frequency.)
    return save_json_file(query, folder="success_query", prefix="q", max_files=None, exp=".q")


def _save_error_query(query):
    # Error query log (Save without limit. Please limit storage depending on the OS type and log frequency.)
    return save_json_file(query, folder="error_query", prefix="q", max_files=None, exp=".q")


def _backup_db():
    # Scheduled backup (keep only latest 7 files, one week, extension .dtdb)
    save_json_file(delta_trace_db.to_dict(), folder="backups", prefix="backup", max_files=7, exp=".dtdb")


def save_json_file(data: dict, folder: str, prefix: str = "log", max_files: int = None, exp: str = ".q"):
    """
    Save JSON data to the specified folder, keeping only max_files old files.
    If max_files=None, do not delete (unlimited).

    Args:
        data: dictionary data to save
        folder: destination folder
        prefix: filename prefix
        max_files: maximum number of files to keep (None means no deletion)
        exp: file extension. Use ".q" for logs, ".dtdb" for DB.
    Returns:
        Path of the saved file
    """
    os.makedirs(folder, exist_ok=True)
    # TODO Change the timezone if necessary.
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%S%f")[:-3]
    unique_id = uuid.uuid4().hex[:8]
    filename = f"{prefix}_{timestamp}_{unique_id}" + exp
    filepath = os.path.join(folder, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        # Save Japanese correctly.
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        # TODO If encryption is needed, encrypt json_str here.
        f.write(json_str)
    # Delete old files if max_files specified
    if max_files is not None and max_files > 0:
        files = [f for f in os.listdir(folder) if f.startswith(prefix) and f.endswith(exp)]
        files.sort()
        if len(files) > max_files:
            for old_file in files[:-max_files]:
                try:
                    os.remove(os.path.join(folder, old_file))
                except Exception as e:
                    print(f"Old backup delete failed.: {old_file}, {e}")
    print("file saved: " + str(filepath))
    return filepath



class ErrorLogHandler(logging.Handler):
    """
    A handler to handle errors when they occur in db.
    """
    def __init__(self):
        super().__init__()

    def emit(self, record):
        try:
            log_entry = self.format(record)
            _save_log(log_entry)
        except Exception:
            self.handleError(record)

# ---------------------------
# Attach the Handler to the DeltaTraceDB logger
# ---------------------------
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

dt_logger = logging.getLogger("delta_trace_db")  # DeltaTraceDB パッケージの親ロガー
dt_logger.setLevel(logging.DEBUG)

handler = ErrorLogHandler()
handler.setFormatter(formatter)
dt_logger.addHandler(handler)

# ---------------------------
# Server startup (with SSL, can run directly with python3 app.py)
# ---------------------------
if __name__ == "__main__":
    uvicorn.run(
        "app:app",  # "filename:FastAPI instance name"
        host="127.0.0.1",
        port=8000,
        reload=False,  # TODO In production, reload must be False. True may cause multiple instances.
        workers=1,  # Single process
        loop="asyncio",  # Use asyncio event loop
        # TODO Refer to another project for SSL configuration. Not handled here.
        ssl_certfile="localhost.crt",  # SSL certificate
        ssl_keyfile="localhost.key"  # SSL private key
    )
