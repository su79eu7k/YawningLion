import datetime
import engine as eng
from fastapi import FastAPI, UploadFile
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
        self.random_cells = {}
        self.probs = {}
        self.monitoring_cells = []

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

        return True

    def check_connection(self):
        try:
            self.workbook.app
            return True
        except Exception as ex:
            print(ex)
            return False

    def simulation(self):
        import time
        time.sleep(20)
        return True


sess = Worker()


class Response(BaseModel):
    code: int
    message: str | None = None


class Selection(Response):
    sheet: str | None = None
    range: str | None = None


class ProbReq(BaseModel):
    start: int | float
    end: int | float
    step: int
    dist: str
    loc: bool = 0
    scale: bool = 1


class ProbRes(Response):
    dist: str
    x: list[float]
    prob: list[float]


class RandomCellAdd(BaseModel):
    sheet: str
    cell: str
    x: list[float]
    prob: list[float]


class RandomCellRemove(BaseModel):
    sheet: str
    cell: str


class MonitoringCellReqs(BaseModel):
    sheet: str
    cell: str
    

app = FastAPI()

origins = [
    "http://127.0.0.1",
    "http://localhost",
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
                "code": 1,
                "message": "Success: Connection, Getting selection."}


@app.post("/prob", response_model=ProbRes)
async def prob(prob_req: ProbReq):
    if prob_req.dist == 'normal':
        x, prob = eng.gen_dist_normal(prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    else:
        x, prob = eng.gen_dist_uniform(prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)

    return {"dist": prob_req.dist,
            "x": x.tolist(),
            "prob": prob.tolist(),
            "code": 1,
            "message": "Success: Variable processed with requested distribution."}


@app.post("/add_random_cell", response_model=Response)
async def add_random_cell(random_cell_add: RandomCellAdd):
    _key = '!'.join([random_cell_add.sheet, random_cell_add.cell])
    sess.random_cells[_key] = random_cell_add.x
    sess.probs[_key] = random_cell_add.prob

    return {"code": 1, "message": f"Success: Assigned."}


@app.post("/remove_random_cell", response_model=Response)
async def remove_random_cell(random_cell_remove: RandomCellRemove):
    _key = '!'.join([random_cell_remove.sheet, random_cell_remove.cell])
    del sess.random_cells[_key]
    del sess.probs[_key]

    return {"code": 1, "message": f"Success: Unassigned."}


@app.post("/add_monitoring_cell", response_model=Response)
async def add_monitoring_cell(monitoring_cell_add: MonitoringCellReqs):
    _key = '!'.join([monitoring_cell_add.sheet, monitoring_cell_add.cell])
    sess.monitoring_cells.append(_key)

    return {"code": 1, "message": f"Success: Assigned."}


@app.post("/remove_monitoring_cell", response_model=Response)
async def remove_monitoring_cell(monitoring_cell_remove: MonitoringCellReqs):
    _key = '!'.join([monitoring_cell_remove.sheet, monitoring_cell_remove.cell])
    sess.monitoring_cells.remove(_key)

    return {"code": 1, "message": f"Success: Assigned."}


@app.get("/check_connection", response_model=Response)
async def check_connection():
    if sess.check_connection():
        return {"code": 1, "message": f"{sess.filename_ext}"}
    else:
        if sess.filename_ext:
            return {"code": 0, "message": f"Disconnected"}
        else:
            return {"code": -1, "message": f"Never connected"}


@app.get("/async_test")
async def async_test():
    import asyncio
    await asyncio.to_thread(sess.simulation)
    return True