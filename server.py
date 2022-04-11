import engine as eng
from fastapi import FastAPI
from pydantic import BaseModel


class Worker:
    def __init__(self):
        self.filepath = None
        self.wb = None

    def connect_workbook(self, filepath):
        try:
            self.wb = eng.xw_load_workbooks(filepath)

            return True
        except FileNotFoundError as ex:

            return False


sess = Worker()


class FileInfo(BaseModel):
    filepath: str


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/connect_workbook")
async def connect_workbook(fileinfo: FileInfo):
    if sess.connect_workbook(fileinfo.filepath):
        return {"message": "Success"}
    else:
        return {"message": "Failed"}
