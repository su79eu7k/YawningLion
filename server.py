import engine as eng
from fastapi import FastAPI
from pydantic import BaseModel


class FileInfo(BaseModel):
    filepath: str


app = FastAPI()


@app.get("/")
async def root():

    return {"message": "Hello World"}


@app.get("/connect_workbook")
async def connect_workbook(fileinfo: FileInfo):

    return {"message": "Hello World"}
