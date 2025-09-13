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
from datetime import datetime
import uuid
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------
# スケジューラの作成
# ---------------------------
scheduler = BackgroundScheduler()
@asynccontextmanager
async def lifespan_context(fastapi_app: FastAPI):
    # Startup 処理
    if not scheduler.running:
        scheduler.add_job(_backup_db, 'cron', hour=1, minute=0, id="daily_backup")
        # TODO テスト時はこちらを有効にすると10秒毎の保存ができます。
        # scheduler.add_job(_backup_db, 'interval', seconds=10, id="test_backup", replace_existing=True)
        scheduler.start()
    yield
    # Shutdown 処理
    scheduler.shutdown()

# ---------------------------
# FastAPIアプリ作成
# ---------------------------
app = FastAPI(lifespan=lifespan_context)


# CORS設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # すべてのオリジンを許可（開発用）
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# インメモリDB（グローバル変数）
# ---------------------------
delta_trace_db = DeltaTraceDatabase()


# ---------------------------
# エンドポイント
# ---------------------------
@app.post("/backend")
async def backend_db(request: Request):
    # TODO まずJWTの検証を行い、正しいユーザーかどうか確認します。

    # 検証が問題なければ、クエリのJSONを取り出します。
    # ここでは、便利に使用するためにクエリクラスを一旦復元します。
    query_json = await request.json()
    query = UtilQuery.convert_from_json(query_json)
    # TODO 操作中のユーザーの権限に応じて、実行できるクエリは制限してください。
    # TODO 許可するもののみを書く方式で、collection_permissions=None（デフォルト）はフロントエンド専用で全て許可となるので注意してください。
    # TODO キーはコレクション名です。パーミッションを指定しているコレクションへのアクセスのみが許可され、それ以外は拒否されます。
    # TODO 例えば以下のパーミッションでは、usersへはaddやgetAllでアクセスできますが、clear等は使えず、users2などのコレクションにもアクセスできません。
    collection_permissions = {"users" : Permission([EnumQueryType.add, EnumQueryType.getAll])}
    result = delta_trace_db.execute_query_object(query=query, collection_permissions=collection_permissions)
    # TODO テスト用。内容確認のためだけにprintしています。本番は削除してください。
    print(str(delta_trace_db.to_dict()))
    # 成功した場合のみ、クエリをログとして保存します。
    if result.is_success:
        # TODO 暗号化などが必要な場合はsave_json_file関数内を調整してください。
        _save_log(query=query_json)
    else:
        # 禁止された呼び出しを行うなどした場合など、
        # 失敗ケースはそのままresultを200 OKで返してもいい(フロントエンドでチェックする場合、isSuccessがfalseになる)ですが、
        # 個別に処理して以下のようにエラーで返すこともできます。
        # エラーになったクエリも調査のために保存しておきます。
        _save_error_query(query=query_json)
        if isinstance(query, Query):
            if not UtilQuery.check_permissions(query, collection_permissions=collection_permissions):
                raise HTTPException(
                    status_code=403,
                    detail="Operation not permitted."
                )
        elif isinstance(query,TransactionQuery):
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
# 自動バックアップやログ保存など
# ---------------------------
def _save_log(query):
    # クエリログ（無制限に保存。OSの種類やログの頻度によっては制限を加えてください）
    return save_json_file(query, folder="logs", prefix="log", max_files=None, exp=".q")

def _save_error_query(query):
    # エラーになったクエリログ（無制限に保存。OSの種類やログの頻度によっては制限を加えてください）
    return save_json_file(query, folder="e_query", prefix="log", max_files=None, exp=".q")

def _backup_db():
    # 定期バックアップ（最新7件（一週間分）のみ、拡張子 .dtdb）
    save_json_file(delta_trace_db.to_dict(), folder="backups", prefix="backup", max_files=7, exp=".dtdb")


def save_json_file(data: dict, folder: str, prefix: str = "log", max_files: int = None, exp: str = ".q"):
    """
    JSONデータを指定フォルダに保存し、古いファイルは max_files 件だけ残して削除
    max_files=None の場合は削除なし（無制限）

    Args:
        data: 保存する辞書データ
        folder: 保存先フォルダ
        prefix: ファイル名の接頭辞
        max_files: 残す最大ファイル数（Noneなら削除なし）
        exp: ファイルの拡張子。ログなら「.q」、dbは「.dtdb」を指定する。
    Returns:
        保存したファイルのパス
    """
    os.makedirs(folder, exist_ok=True)
    now = datetime.now()
    timestamp = now.strftime("%Y%m%dT%H%M%S%f")[:-3]
    unique_id = uuid.uuid4().hex[:8]
    filename = f"{prefix}_{timestamp}_{unique_id}" + exp
    filepath = os.path.join(folder, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        # 日本語を有効なまま保存。
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        # TODO 暗号化が必要な場合、ここのjson_strを暗号化してください。
        f.write(json_str)
    # max_files が指定されていれば古いファイルを削除
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

# ---------------------------
# サーバー起動（SSL付き、python3 app_ja.pyで直接起動可能）
# ---------------------------
if __name__ == "__main__":
    uvicorn.run(
        "app:app",  # "ファイル名:FastAPIインスタンス名"
        host="127.0.0.1",
        port=8000,
        reload=False,  # TODO 本番環境ではリロードしてはならないのでFalseにします。Trueだと多重起動する可能性があります。
        workers=1,  # シングルプロセス
        loop="asyncio",  # asyncioイベントループを使う
        # TODO SSLの設定は別のプロジェクト等を参照してください。この例では扱いません。
        ssl_certfile="localhost.crt",  # SSL証明書
        ssl_keyfile="localhost.key"  # SSL秘密鍵
    )
