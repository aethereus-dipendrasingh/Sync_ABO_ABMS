from fastapi import FastAPI
from typing import Optional

app = FastAPI()

# Sample data (you can replace this with DB queries or external sources)
items = {
    1: {"name": "Item One", "price": 100},
    2: {"name": "Item Two", "price": 200},
}


@app.get("/")
def read_root():
    return {"message": "Welcome to the API!"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    item = items.get(item_id)
    if not item:
        return {"error": "Item not found"}

    if q:
        return {"item": item, "query": q}
    
    return {"item": item}