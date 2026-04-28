from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import time

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# This dictionary stores our DataNode "Objects"
# In a real system, these would come from gRPC calls
datanodes = {}
@app.post("/api/register")
async def register_node(hb: Heartbeat):
    # If it's a new node, it gets a "Slot" in your UI automatically
    if hb.ip not in datanodes:
        datanodes[hb.ip] = {
            "id": len(datanodes) + 1,
            "status": "Active",
            "first_seen": time.time(),
            "last_ping": time.time(),
            "storage": hb.storage_used
        }
    return {"status": "registered"}

class Heartbeat(BaseModel):
    ip: str
    storage_used: str

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    # Try passing the request specifically like this:
    return templates.TemplateResponse(
        request=request, 
        name="index.html"
    )
@app.get("/api/status")
async def get_status():
    current_time = time.time()
    # Calculate if node is "Dead" (no ping for 10 seconds)
    for ip in datanodes:
        diff = current_time - datanodes[ip]["last_ping"]
        if diff > 10:
            datanodes[ip]["status"] = "Dead"
        else:
            datanodes[ip]["status"] = "Active"
    return datanodes

@app.post("/api/heartbeat")
async def receive_heartbeat(hb: Heartbeat):
    if hb.ip in datanodes:
        datanodes[hb.ip]["last_ping"] = time.time()
        datanodes[hb.ip]["storage"] = hb.storage_used
        return {"status": "success"}
    return {"status": "node_not_found"}