import grpc
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from proto import namenode_pb2, namenode_pb2_grpc
from configparser import ConfigParser
import os

app = FastAPI()
# 1. Locate the config file
# Moving up two levels from client/WEB-APP/ to client/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(BASE_DIR, "config", "client.config")

# 2. Parse the specific [cluster] section
config = ConfigParser()
config.read(config_path)
NAMENODE_ADDR = f"{config.get('cluster', 'namenode_address')}:{config.get('cluster', 'namenode_port')}"

print(f"WEB_APP: Connecting to NameNode at {NAMENODE_ADDR}")

templates = Jinja2Templates(directory="client/WEB-APP/templates")

@app.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request):
        return templates.TemplateResponse(
            request=request, 
            name="index.html"
        )
@app.get("/api/status")
async def get_cluster_status():
    try:
        # Connect to NameNode gRPC
        with grpc.insecure_channel(NAMENODE_ADDR) as channel:
            stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
            # Timeout is vital: if NameNode is dead, we want to know immediately
            response = stub.GetClusterStatus(namenode_pb2.ClusterStatusRequest(), timeout=1.5)
            
            # Reformat for the JS frontend
            formatted_nodes = {}
            for node in response.nodes:
                formatted_nodes[node.node_id] = {
                    "ip": f"{node.hostname}:{node.port}",
                    "status": node.status,
                    "storage": f"{node.storage_used_pct:.1f}%"
                }
            return {
                "namenode_online": response.namenode_active,
                "nodes": formatted_nodes
            }
    except Exception as e:
        # This triggers if NameNode service is down or unreachable
        print(f"\r ===========NameNode: Unreachable============                   ",end="",flush=True)
        return {"namenode_online": False, "nodes": {}}