import datetime
import numpy as np
import engine as eng
import asyncio
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

        self.workbook_obj = None

        self.random_cells = {}
        self.probs = {}
        self.trial_cells = {}
        self.monitoring_cells = {}
        self.chunk_processed = None

        self.chunks = None
        self.progress = None
        self.task = None

    def connect_workbook(self, fullpath):
        try:
            self.workbook_obj = eng.xw_load_workbooks(fullpath)

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
        return eng.xw_get_selection(self.workbook_obj).replace("'", "").split('!')

    def check_connection(self):
        try:
            _ = self.workbook_obj.app
            return True
        except Exception as ex:
            print(ex)
            return False

    def random_sampling(self, num_trials):
        for k in self.random_cells.keys():
            _prob = np.array([p / np.sum(self.probs[k]) for p in self.probs[k]])
            self.trial_cells[k] = np.random.choice(self.random_cells[k], num_trials, p=_prob)

        return True

    async def run_simulation(self, num_trials=2000, num_chunk=10, resume=False):
        if not resume:
            self.random_sampling(num_trials=num_trials)
            self.chunks = eng.util_build_chunks(list(range(num_trials)), num_chunk)

            self.progress = 0
            self.chunk_processed = 0

        for c in self.chunks[self.chunk_processed:]:
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                print(f'Cancelled at Chunk-{self.chunk_processed}.')
                raise
            for n in c:
                for k in self.trial_cells.keys():
                    _sheet, _cell = k.split('!')
                    self.workbook_obj.sheets(_sheet).range(_cell).value = self.trial_cells[k][n]

                for k in self.monitoring_cells.keys():
                    _sheet, _cell = k.split('!')
                    self.monitoring_cells[k].append(self.workbook_obj.sheets(_sheet).range(_cell).value)
            self.chunk_processed += 1
            self.progress = self.chunk_processed / len(self.chunks)

        return True

    async def stop_simulation(self, cancel=False):
        sess.task.cancel()
        while not sess.task.cancelled():
            await asyncio.sleep(.1)

        if cancel:
            self.trial_cells = {}
            self.chunk_processed = None

            self.chunks = None
            self.progress = None
            self.task = None

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
    a: int | float | None
    b: int | float | None
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


class ProcSimReq(BaseModel):
    num_trials: int


class Progress(Response):
    progress: float | None


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


@app.get("/reset", response_model=Response)
async def reset():
    sess.__init__()

    return {"code": 1, "message": "Success: Session re-initiated."}


@app.post("/upload_file/", response_model=Response)
async def upload_file(uploadfile: UploadFile):
    sess.init_workbook(uploadfile)
    sess.connect_workbook(sess.fullpath)
    sess.get_selection()

    return {"code": 1,
            "message": "Success: Workbook initiation, Connection, Getting selection."}


@app.get("/get_selection", response_model=Selection)
async def get_selection():
    _sheet, _cell = sess.get_selection()

    if ':' in _cell:
        return {"sheet": _sheet,
                "range": "WideRange",
                "code": 0,
                "message": "Success: Connection. Failed: Getting selection(Too wide)."}
    else:
        return {"sheet": _sheet,
                "range": _cell,
                "code": 1,
                "message": "Success: Connection, Getting selection."}


@app.post("/prob", response_model=ProbRes)
async def prob(prob_req: ProbReq):
    if prob_req.dist in ['norm', 'normal', 'gauss', 'gaussian']:
        x, prob = eng.stat_gen_dist_normal(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['exp', 'expon', 'exponential']:
        x, prob = eng.stat_gen_dist_exponential(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['beta']:
        x, prob = eng.stat_gen_dist_beta(
            prob_req.start, prob_req.end, prob_req.step, prob_req.a, prob_req.b, prob_req.loc, prob_req.scale)
    else:
        x, prob = eng.stat_gen_dist_uniform(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)

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
    sess.monitoring_cells[_key] = []

    return {"code": 1, "message": f"Success: Assigned."}


@app.post("/remove_monitoring_cell", response_model=Response)
async def remove_monitoring_cell(monitoring_cell_remove: MonitoringCellReqs):
    _key = '!'.join([monitoring_cell_remove.sheet, monitoring_cell_remove.cell])
    del sess.monitoring_cells[_key]

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


@app.post("/proc_sim", response_model=Response)
async def proc_sim(proc_sim_req: ProcSimReq):
    sess.task = asyncio.create_task(sess.run_simulation(num_trials=proc_sim_req.num_trials))
    try:
        await sess.task
    except asyncio.CancelledError:
        print('Initial task cancelled.')

    return {"code": 1, "message": f"Succcess"}


@app.get("/cancel_sim", response_model=Response)
async def cancel_sim():
    res = asyncio.create_task(sess.stop_simulation(cancel=True))
    await res

    return {"code": 1, "message": f"Succcess"}


@app.get("/pause_sim", response_model=Response)
async def pause_sim():
    res = asyncio.create_task(sess.stop_simulation(cancel=False))
    await res

    return {"code": 1, "message": f"Succcess"}


@app.get("/resume_sim", response_model=Response)
async def resume_sim():
    sess.task = asyncio.create_task(sess.run_simulation(resume=True))
    try:
        await sess.task
    except asyncio.CancelledError:
        print('Resumed task cancelled.')

    return {"code": 1, "message": f"Succcess"}


@app.get("/get_progress", response_model=Progress)
async def get_progress():
    if sess.progress is None:
        return {"progress": None, "code": 0, "message": f"Failed: Not even 0%."}
    else:
        return {"progress": sess.progress, "code": 1, "message": f"{sess.progress * 100}%."}
