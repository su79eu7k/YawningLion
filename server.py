import engine as eng
from fastapi import FastAPI
from pydantic import BaseModel


class Worker:
    def __init__(self):
        self.filepath = None
        self.workbook = None
        self.worksheet = None
        self.range = None
        self.value = None

    def connect_workbook(self, filepath):
        try:
            self.workbook = eng.xw_load_workbooks(filepath)

            return True
        except FileNotFoundError as ex:

            return False

    def get_selection(self):
        _selection = eng.xw_get_selection(self.workbook).split('!')
        self.worksheet = _selection[0].replace("'", "")
        self.range = _selection[1]
        self.value = self.workbook.sheets[self.worksheet].range(self.range).value

        return True


sess = Worker()


class Message(BaseModel):
    code: int = 9
    message: str | None = None


class FileInfo(BaseModel):
    filepath: str


class Selection(Message):
    range: str | None = None
    value: float | None = None


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
        if sess.range:
            if ':' in sess.range:
                return {"range": "WideRange", "code": 0, "message": "Failed: Selection is too wide."}
            else:
                return {"range": sess.range, "value": sess.value, "code": 1, "message": "Success"}
        else:
            return {"code": 0, "message": "Failed: No selection."}
    else:
        return {"code": 0, "message": "Failed: Workbook disconnected."}

