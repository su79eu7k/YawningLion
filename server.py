import time
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
        self.w_dir = './workbooks/'
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

        self.throughput = None

    def connect_workbook(self, fullpath):
        try:
            self.workbook_obj = eng.xw_load_workbooks(fullpath)

            return True
        except FileNotFoundError as ex:
            print(ex)

            return False

    def init_workbook(self, uploadfile):
        self.ext = '.' + uploadfile.filename.split('.')[-1]
        self.filename = f"SStorm_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.filename_ext = self.filename + self.ext
        self.fullpath = self.w_dir + self.filename_ext

        with open(self.fullpath, 'wb+') as f:
            f.write(uploadfile.file.read())

        return True

    def get_selection(self):
        return eng.xw_get_selection(self.workbook_obj).replace("'", "").split('!')

    def select_with_focus(self, address_sheet, address_cell):
        eng.xw_select_with_focus(self.workbook_obj, address_sheet, address_cell)

        return True

    def check_connection(self):
        try:
            _ = self.workbook_obj.app
            return True
        except Exception as ex:
            print(ex)
            return False

    def run_benchmark(self, num_trials=20):
        # self.trial_cells reset by random_sampling() but self.monitoring_cells doesn't.
        for k in self.random_cells.keys():
            _prob = np.array([p / np.sum(self.probs[k]) for p in self.probs[k]])
            self.trial_cells[k] = np.random.choice(self.random_cells[k], num_trials, p=_prob)

        for k in self.monitoring_cells.keys():
            self.monitoring_cells[k] = []

        time_start = time.time()
        for n in range(num_trials):
            for k in self.trial_cells.keys():
                _sheet, _cell = k.split('!')
                self.workbook_obj.sheets(_sheet).range(_cell).value = self.trial_cells[k][n]
            self.workbook_obj.app.calculate()

            for k in self.monitoring_cells.keys():
                _sheet, _cell = k.split('!')
                self.monitoring_cells[k].append(self.workbook_obj.sheets(_sheet).range(_cell).value)
        time_elapsed = time.time() - time_start
        self.throughput = num_trials / time_elapsed

        return True

    async def run_simulation(self, async_sleep=0.1, num_chunk=20, num_trials=2000, resume=False):
        if not resume:
            # self.trial_cells reset by random_sampling() but self.monitoring_cells doesn't.
            for k in self.random_cells.keys():
                _prob = np.array([p / np.sum(self.probs[k]) for p in self.probs[k]])
                self.trial_cells[k] = np.random.choice(self.random_cells[k], num_trials, p=_prob)

            for k in self.monitoring_cells.keys():
                self.monitoring_cells[k] = []

            self.chunks = eng.util_build_chunks(list(range(num_trials)), num_chunk)

            self.chunk_processed = 0
            self.progress = 0

        for c in self.chunks[self.chunk_processed:]:
            try:
                await asyncio.sleep(async_sleep)
            except asyncio.CancelledError:
                print(f'Cancelled at Chunk-{self.chunk_processed}.')
                raise
            for n in c:
                for k in self.trial_cells.keys():
                    _sheet, _cell = k.split('!')
                    self.workbook_obj.sheets(_sheet).range(_cell).value = self.trial_cells[k][n]
                self.workbook_obj.app.calculate()

                for k in self.monitoring_cells.keys():
                    _sheet, _cell = k.split('!')
                    self.monitoring_cells[k].append(self.workbook_obj.sheets(_sheet).range(_cell).value)
            self.chunk_processed += 1
            self.progress = self.chunk_processed / len(self.chunks)

        return True

    async def stop_simulation(self, cancel=False):
        sess.task.cancel()
        while not sess.task.cancelled():
            await asyncio.sleep(.5)

        if cancel:
            self.trial_cells = {}
            self.chunk_processed = None

            self.chunks = None
            self.progress = None
            self.task = None

        return True


sess = Worker()
sess_lock = asyncio.Lock()


class Response(BaseModel):
    code: int
    message: str | None = None


class Selection(Response):
    sheet: str | None = None
    range: str | None = None


class ProbReq(BaseModel):
    dist: str
    start: int | float
    end: int | float
    step: int
    loc: float | None
    scale: float | None
    a: float | None
    b: float | None


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


class PreviewDataReq(BaseModel):
    x: str
    y: str


class PreviewDataXY(BaseModel):
    x: float | None = None
    y: float | None = None


class PreviewDataRes(Response):
    xy: list[PreviewDataXY]


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
    async with sess_lock:
        sess.__init__()

    return {"code": 1, "message": "Success: Session re-initiated."}


@app.post("/upload_file/", response_model=Response)
async def upload_file(uploadfile: UploadFile):
    async with sess_lock:
        sess.init_workbook(uploadfile)
        sess.connect_workbook(sess.fullpath)
        sess.get_selection()

    return {"code": 1,
            "message": "Success: Workbook initiation, Connection, Getting selection."}


@app.get("/get_selection", response_model=Selection)
async def get_selection():
    _sheet, _cell = sess.get_selection()

    # Wide-Range handling.
    if ':' in _cell:
        _cell = _cell.split(':')[0]

    return {"sheet": _sheet,
            "range": _cell,
            "code": 1,
            "message": "Success: Connection, Getting selection."}


@app.get("/select_with_focus/", response_model=Response)
async def select_with_focus(sheet: str, cell: str):
    sess.select_with_focus(sheet, cell)

    return {"code": 1, "message": "Success."}


@app.post("/prob", response_model=ProbRes)
async def prob(prob_req: ProbReq):
    if prob_req.dist in ['norm', 'normal', 'gauss', 'gaussian']:
        x, p = eng.stat_gen_dist_normal(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['exp', 'expon', 'exponential']:
        x, p = eng.stat_gen_dist_exponential(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['beta']:
        x, p = eng.stat_gen_dist_beta(
            prob_req.start, prob_req.end, prob_req.step, prob_req.a, prob_req.b, prob_req.loc, prob_req.scale)
    else:
        x, p = eng.stat_gen_dist_uniform(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)

    return {"dist": prob_req.dist,
            "x": x.tolist(),
            "prob": p.tolist(),
            "code": 1,
            "message": "Success: Variable processed with requested distribution."}


@app.post("/add_random_cell", response_model=Response)
async def add_random_cell(random_cell_add: RandomCellAdd):
    async with sess_lock:
        _key = '!'.join([random_cell_add.sheet, random_cell_add.cell])
        sess.random_cells[_key] = random_cell_add.x
        sess.probs[_key] = random_cell_add.prob

    return {"code": 1, "message": f"Success: Assigned."}


@app.post("/remove_random_cell", response_model=Response)
async def remove_random_cell(random_cell_remove: RandomCellRemove):
    async with sess_lock:
        _key = '!'.join([random_cell_remove.sheet, random_cell_remove.cell])
        del sess.random_cells[_key]
        del sess.probs[_key]

    return {"code": 1, "message": f"Success: Unassigned."}


@app.post("/add_monitoring_cell", response_model=Response)
async def add_monitoring_cell(monitoring_cell_add: MonitoringCellReqs):
    async with sess_lock:
        _key = '!'.join([monitoring_cell_add.sheet, monitoring_cell_add.cell])
        sess.monitoring_cells[_key] = []

    return {"code": 1, "message": f"Success: Assigned."}


@app.post("/remove_monitoring_cell", response_model=Response)
async def remove_monitoring_cell(monitoring_cell_remove: MonitoringCellReqs):
    async with sess_lock:
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
    sess.workbook_obj.app.screen_updating = False
    sess.workbook_obj.app.calculation = 'manual'

    sess.run_benchmark()

    # API calls: 2 times / 3 sec, takes 5ms each.
    _async_sleep = .02
    _max_blocking = 1.5
    if sess.throughput:
        _num_chunk = max(round(sess.throughput / (1 / _max_blocking)), 1)
    else:
        _num_chunk = 5

    print(_num_chunk)

    sess.task = asyncio.create_task(
        sess.run_simulation(async_sleep=_async_sleep, num_chunk=_num_chunk, num_trials=proc_sim_req.num_trials))
    try:
        await sess.task
    except asyncio.CancelledError:
        print('Initial task cancelled.')

    sess.workbook_obj.app.screen_updating = True
    sess.workbook_obj.app.calculation = 'automatic'

    return {"code": 1, "message": f"Success"}


@app.get("/cancel_sim", response_model=Response)
async def cancel_sim():
    res = asyncio.create_task(sess.stop_simulation(cancel=True))
    await res

    sess.workbook_obj.app.screen_updating = True
    sess.workbook_obj.app.calculation = 'automatic'

    return {"code": 1, "message": f"Success"}


@app.get("/pause_sim", response_model=Response)
async def pause_sim():
    res = asyncio.create_task(sess.stop_simulation(cancel=False))
    await res

    sess.workbook_obj.app.screen_updating = True
    sess.workbook_obj.app.calculation = 'automatic'

    return {"code": 1, "message": f"Success"}


@app.get("/resume_sim", response_model=Response)
async def resume_sim():
    sess.workbook_obj.app.screen_updating = False
    sess.workbook_obj.app.calculation = 'manual'

    sess.task = asyncio.create_task(sess.run_simulation(resume=True))
    try:
        await sess.task
    except asyncio.CancelledError:
        print('Resumed task cancelled.')

    sess.workbook_obj.app.screen_updating = True
    sess.workbook_obj.app.calculation = 'automatic'

    return {"code": 1, "message": f"Success"}


@app.get("/get_progress", response_model=Progress)
async def get_progress():
    if sess.progress is None:
        return {"progress": None, "code": 0, "message": f"Failed: Not even 0%."}
    else:
        return {"progress": sess.progress, "code": 1, "message": f"{sess.progress * 100}%."}


@app.post("/preview_data", response_model=PreviewDataRes)
async def preview_data(preview_data_req: PreviewDataReq):
    _type_x, _x = preview_data_req.x.split("'")
    _type_y, _y = preview_data_req.y.split("'")

    x = []
    if _type_x == 'rand':
        x = sess.trial_cells[_x].tolist()
    elif _type_x == 'monit':
        x = sess.monitoring_cells[_x]

    y = []
    if _type_y == 'rand':
        y = sess.trial_cells[_y].tolist()
    elif _type_y == 'monit':
        y = sess.monitoring_cells[_y]

    _c = min(len(x), len(y))
    x = x[:_c]
    y = y[:_c]

    xy = [{"x": n[0], "y": n[1]} for n in zip(x, y)]

    return {"code": 1, "message": f"Success", "xy": xy}
