import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# This is the security bypass so your future map can read the data
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "Server is running on Port 8080!"}

@app.get("/api/news")
def get_news():
    with open("live_news.geojson", "r") as f:
        data = json.load(f)
    return data

if __name__ == "__main__":
    # We changed the port to 8080 to avoid the previous error!
    uvicorn.run(app, host="127.0.0.1", port=8080)