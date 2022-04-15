import datetime
import engine as eng
from fastapi import FastAPI, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


class Worker:
    def __init__(self):
        self.ext = None
        self.filename = None
        self.filename_ext = None
        self.wdir = './workbooks/'
        self.fullpath = None
        self.workbook = None
        self.worksheet = None
        self.range = None
        self.value = None
        self.variables = {}
        self.probs = {}

    def connect_workbook(self, fullpath):
        try:
            self.workbook = eng.xw_load_workbooks(fullpath)

            return True
        except FileNotFoundError as ex:

            return False

    def init_workbook(self, uploadfile):
        self.ext = '.' + uploadfile.filename.split('.')[-1]
        self.filename = f"SStorm_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.filename_ext = self.filename + self.ext
        self.fullpath = self.wdir + self.filename_ext

        with open(self.fullpath, 'wb+') as f:
            f.write(uploadfile.file.read())

        return True

    def get_selection(self):
        _selection = eng.xw_get_selection(self.workbook).split('!')
        self.worksheet = _selection[0].replace("'", "")
        self.range = _selection[1]
        self.value = self.workbook.sheets[self.worksheet].range(self.range).value

        return True

    def check_connection(self):
        try:
            self.workbook.app
            return True
        except Exception as ex:
            print(ex)
            return False


sess = Worker()


class Response(BaseModel):
    code: int
    message: str | None = None


class Selection(Response):
    sheet: str | None = None
    range: str | None = None
    value: int | float | str | None = None


class VarIn(BaseModel):
    start: int | float
    end: int | float
    num: int
    dist: str
    loc: bool = 0
    scale: bool = 1


class VarOut(Response):
    dist: str
    x: list[float]
    prob: list[float]


class VarCommit(BaseModel):
    range: str
    x: list[float]
    prob: list[float]


app = FastAPI()

origins = [
    "http://127.0.0.1:3000",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/upload_file/", response_model=Response)
async def upload_file(uploadfile: UploadFile):
    sess.init_workbook(uploadfile)
    sess.connect_workbook(sess.fullpath)
    sess.get_selection()

    return {"code": 1,
            "message": "Success: Workbook initiation, Connection, Getting selection."}


@app.get("/get_selection", response_model=Selection)
async def get_selection():
    sess.get_selection()

    if ':' in sess.range:
        return {"sheet": sess.worksheet,
                "range": "WideRange",
                "code": 0,
                "message": "Success: Connection. Failed: Getting selection(Too wide)."}
    else:
        return {"sheet": sess.worksheet,
                "range": sess.range,
                "value": sess.value,
                "code": 1,
                "message": "Success: Connection, Getting selection."}


@app.post("/io_variable", response_model=VarOut)
async def io_variable(var: VarIn):
    x, prob = eng.gen_dist_uniform(var.start, var.end, var.num, var.loc, var.scale)

    return {"dist": var.dist,
            "x": x.tolist(),
            "prob": prob.tolist(),
            "code": 1,
            "message": "Success: Variable processed with requested distribution."}


@app.post("/commit_variable")
async def commit_variable(variable: VarCommit):
    sess.variables[variable.range] = variable.x
    sess.probs[variable.range] = variable.prob


@app.get("/check_connection", response_model=Response)
async def check_connection():
    if sess.check_connection():
        return {"code": 1, "message": f"{sess.filename_ext}"}
    else:
        if sess.filename_ext:
            return {"code": 0, "message": f"Disconnected"}
        else:
            return {"code": -1, "message": f"Never connected"}
