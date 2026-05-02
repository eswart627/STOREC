import os
import sys
import queue
import shutil
import tempfile

from threading import Thread
from pydantic import BaseModel

from fastapi import (FastAPI, UploadFile, File, Request)
from fastapi.responses import (HTMLResponse, StreamingResponse)
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from client.app.client import upload_file
from client.app.read_file import read_file
from client.app.delete_file import delete_file
from client.app.namenode_client import NameNodeClient
from client.app.config_loader import MAX_WORKERS

progress_queue = queue.Queue()

def clear_queue():
    while not progress_queue.empty():
        try:
            progress_queue.get_nowait()
        except queue.Empty:
            break


def log_progress(message):
    progress_queue.put(message)


class QueueLogger:
    def write(self, message):
        message = message.strip()
        if message:
            progress_queue.put(message)
    def flush(self):
        pass

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

templates = Jinja2Templates(
    directory=os.path.join(
        BASE_DIR,
        "templates"
    )
)

app.mount(
    "/static",
    StaticFiles(directory=os.path.join(BASE_DIR, "static")),
    name="static"
)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html"
    )


@app.get("/api/files")
async def list_files():
    try:
        namenode = NameNodeClient()
        files = namenode.list_files()
        return {
            "files": files
        }
    except Exception as e:
        return {
            "files": [],
            "error": str(e)
        }


@app.post("/api/upload")
async def upload_api(file: UploadFile = File(...)):
    clear_queue()
    temp_path = os.path.join(
        tempfile.gettempdir(),
        file.filename
    )
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(
            file.file,
            buffer
        )

    def run_upload():
        logger = QueueLogger()
        old_stdout = sys.stdout

        try:
            sys.stdout = logger

            log_progress(
                f"Uploading: {file.filename}"
            )

            upload_file(
                temp_path,
                mode="parallel",
                max_workers=MAX_WORKERS
            )

            log_progress(
                "Upload completed successfully"
            )
        except Exception as e:
            log_progress(
                f"ERROR: {str(e)}"
            )
        finally:
            sys.stdout = old_stdout

    Thread(
        target=run_upload,
        daemon=True
    ).start()

    return {
        "status": "started"
    }


@app.get("/api/progress")
def stream_progress():
    def event_stream():
        while True:
            try:
                message = progress_queue.get(
                    timeout=0.1
                )

                yield f"data: {message}\n\n"
            except queue.Empty:
                continue
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream"
    )

class ReadRequest(BaseModel):
    file_name: str


@app.post("/api/read")
async def read_api(req: ReadRequest):
    file_name = req.file_name
    try:
        output_dir = os.path.join(BASE_DIR, "downloads")
        os.makedirs(output_dir,exist_ok=True)
        output_path = os.path.join(output_dir, file_name)

        read_file(file_name, output_path)

        return {
            "status": "success",
            "path": output_path
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

class DeleteRequest(BaseModel):
    file_name: str

@app.delete("/api/delete")
async def delete_api(req: DeleteRequest):
    file_name = req.file_name

    try:
        delete_file(file_name)

        return {
            "status": "deleted"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }