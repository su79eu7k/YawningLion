import engine as eng
from fastapi import FastAPI
from pydantic import BaseModel


class Worker:
    def __init__(self):
        self.filepath = None
        self.workbook = None
        self.selection = None

    def connect_workbook(self, filepath):
        try:
            self.workbook = eng.xw_load_workbooks(filepath)

            return True
        except FileNotFoundError as ex:

            return False

    def get_selection(self):
        self.selection = eng.xw_get_selection(self.workbook)

        return True


sess = Worker()


class Message(BaseModel):
    code: int = 9
    message: str | None = None


class FileInfo(BaseModel):
    filepath: str


class Selection(Message):
    address: str


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/connect_workbook", response_model=Message)
async def connect_workbook(fileinfo: FileInfo):
    # if sess.connect_workbook(fileinfo.filepath):
    if sess.connect_workbook('D:/Localhome/sekim/OneDrive - ZF Friedrichshafen AG/Desktop/NPV concept.xlsx'):
        return {"code": 1, "message": "Success"}
    else:
        return {"code": 0, "message": "Failed"}


@app.get("/get_selection", response_model=Selection)
async def get_selection():
    if sess.workbook:
        sess.get_selection()
        if sess.selection:
            if ':' in sess.selection:
                return {"address": "Range", "code": 0, "message": "Failed: Selection too wide"}
            else:
                return {"address": sess.selection, "code": 1, "message": "Success"}
        else:
            return {"address": "None", "code": 0, "message": "Failed: No selection"}

