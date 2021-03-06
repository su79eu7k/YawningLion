import io
import os
import time
import hashlib
import json
from math import ceil
import numpy as np
from pandas import DataFrame
from xlwings import apps, Book
import asyncio
from fastapi import FastAPI, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import MetaData, Table, Column, String, Float, Integer, select, insert, update, delete, func, exists
from sqlalchemy.ext.asyncio import create_async_engine
import dists


engine = create_async_engine(
    "sqlite+aiosqlite:///simulations.db", echo=False, future=True
)

metadata_obj = MetaData()
snapshots_table = Table(
    "snapshots",
    metadata_obj,
    Column("filename", String),
    Column("hash_params", String),
    Column("saved", Float),
    Column("cell_type", String),
    Column("cell_address", String),
    Column("loop", Integer),
    Column("hash_records", String),
    Column("cell_value", Float),
)

params_table = Table(
    "params",
    metadata_obj,
    Column("hash_params", String),
    Column("param_type", String),
    Column("cell_address", String),
    Column("param_index", Integer),
    Column("param_value", Float),
)

alias_table = Table(
    "alias",
    metadata_obj,
    Column("hash_params", String),
    Column("cell_type", String),
    Column("cell_address", String),
    Column("cell_alias", String),
    Column("cell_description", String),
)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(metadata_obj.create_all)


class Worker:
    def __init__(self):
        self.ext = None
        self.filename = None
        self.filename_ext = None
        self.w_dir = './workbooks/'
        self.fullpath = None

        self.workbook_obj = None
        self.pid = None

        self.random_cells = {}
        self.probs = {}
        self.trial_cells = {}
        self.monitoring_cells = {}
        self.random_cells_aliases = {}
        self.monitoring_cells_aliases = {}

        self.chunks = None
        self.chunk_processed = None
        self.progress = None
        self.task = None

        self.throughput = None

        self.hash_params = None
        self.saved = None


    def get_selection(self):
        self.workbook_obj.activate()
        return self.workbook_obj.selection.get_address(False, False, True, False).replace("'", "").split('!')


    def select_with_focus(self, address_sheet, address_cell):
        _sheet = self.workbook_obj.sheets[address_sheet]
        _sheet.activate()
        _sheet.range(address_cell).select()
        self.workbook_obj.activate(steal_focus=True)

        return True

    def get_hash_records(self, ts_saved, loop):
        _family_identifier = {
            "filename": self.filename_ext,
            "hash_params": self.hash_params,
            "ts_saved": ts_saved,
            "loop": loop
        }

        return hashlib.md5(json.dumps(_family_identifier).encode('utf-8')).hexdigest()

    def get_hash_params(self):
        _params = {
            "filename": self.filename_ext,
            "random_cells": dict(sorted(self.random_cells.items())),
            "probs": dict(sorted(self.probs.items())),
            "monitoring_cells": {k: [] for k, _ in sorted(self.monitoring_cells.items())},
            "random_cells_aliases": dict(sorted(sess.random_cells_aliases.items())),
            "monitoring_cells_aliases": dict(sorted(sess.monitoring_cells_aliases.items()))
        }

        return hashlib.md5(json.dumps(_params).encode('utf-8')).hexdigest()

    def connect_workbook(self, fullpath):
        try:
            self.workbook_obj = Book(fullpath)
            self.pid = self.workbook_obj.app.pid

            return True
        except FileNotFoundError as ex:
            print(ex)

            return False

    def init_workbook(self, uploadfile):
        self.ext = '.' + uploadfile.filename.split('.')[-1]
        self.filename = "".join(uploadfile.filename.split('.')[:-1])
        self.filename_ext = self.filename + self.ext
        self.fullpath = self.w_dir + self.filename_ext

        try:
            if os.path.exists(self.fullpath):
                os.remove(self.fullpath)
        except PermissionError as ex:
            return False

        with open(self.fullpath, 'wb+') as f:
            f.write(uploadfile.file.read())

        return True

    def check_connection(self):
        if self.pid in apps.keys():
            return True
        else:
            return False

    def run_benchmark(self, num_trials=20):
        # self.trial_cells reset by random_sampling() but self.monitoring_cells doesn't.
        self.trial_cells = {}
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

    def util_build_chunks(self, lst, size):
        return list(map(lambda x: lst[x * size:x * size + size], list(range(0, ceil(len(lst) / size)))))

    async def run_simulation(self, async_sleep=0.1, num_chunk=20, num_trials=2000, resume=False):
        if not resume:
            # self.trial_cells reset by random_sampling() but self.monitoring_cells doesn't.
            self.trial_cells = {}
            for k in self.random_cells.keys():
                _prob = np.array([p / np.sum(self.probs[k]) for p in self.probs[k]])
                self.trial_cells[k] = np.random.choice(self.random_cells[k], num_trials, p=_prob)

            for k in self.monitoring_cells.keys():
                self.monitoring_cells[k] = []

            self.chunks = self.util_build_chunks(list(range(num_trials)), num_chunk)

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
    step: int | None
    loc: float | None
    scale: float | None
    a: float | None
    b: float | None
    p: float | None
    mu: float | None


class ProbRes(Response):
    dist: str
    x: list[float]
    prob: list[float]


class RandomCellAdd(BaseModel):
    sheet: str
    cell: str
    x: list[float]
    prob: list[float]
    alias: str | None
    description: str | None


class RandomCellRemove(BaseModel):
    sheet: str
    cell: str


class MonitoringCellReqs(BaseModel):
    sheet: str
    cell: str
    alias: str | None
    description: str | None


class RunSimStartReq(BaseModel):
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


class HistListRes(BaseModel):
    filename: str
    hash_params: str
    saved: float
    samples: int


class HistListParamsRes(BaseModel):
    filename: str
    hash_params: str
    random: int
    monitoring: int


class DelHistSimReq(BaseModel):
    filename: str
    hash_params: str


class HistSimCorrRes(BaseModel):
    x: str
    y: str
    v: float


class HistSimSummaryRes(BaseModel):
    column: str
    stats: str
    value: float


class HistSimSummaryReq(BaseModel):
    hash_params: str
    cell_type: str | None
    cell_address: str | None
    cell_value_egt: float | None
    cell_value_elt: float | None


class HistSimParamsRes(BaseModel):
    param_type: str
    cell_address: str
    param_index: int | None
    param_value: float | None


class HistSimRecsReq(BaseModel):
    hash_params: str
    scoped_cell_type: str | None
    scoped_cell_address: str | None
    scoped_cell_value_egt: float | None
    scoped_cell_value_elt: float | None


class Alias(BaseModel):
    hash_params: str
    cell_type: str
    cell_address: str
    cell_alias: str
    cell_description: str


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


@app.on_event("startup")
async def startup_event():
    await init_db()


@app.on_event("shutdown")
async def shutdown_event():
    await engine.dispose()


@app.get("/reset", response_model=Response, tags=["Connection"])
async def reset():
    async with sess_lock:
        sess.__init__()

    return {"code": 1, "message": "Success: Session re-initiated."}


@app.post("/upload_file/", response_model=Response, tags=["Connection"])
async def upload_file(uploadfile: UploadFile):
    async with sess_lock:
        if sess.init_workbook(uploadfile):
            sess.connect_workbook(sess.fullpath)
            sess.get_selection()

            return {"code": 1,
                    "message": "Success: Workbook initiation, Connection, Getting selection."}
        else:
            return {"code": 0,
                    "message": "Fail: Workbook initiation error."}


@app.get("/get_selection", response_model=Selection, tags=["Params"])
async def get_selection():
    _sheet, _cell = sess.get_selection()

    # Wide-Range handling.
    if ':' in _cell:
        _cell = _cell.split(':')[0]

    return {"sheet": _sheet,
            "range": _cell,
            "code": 1,
            "message": "Success: Connection, Getting selection."}


@app.get("/select_with_focus/", response_model=Response, tags=["Params"])
async def select_with_focus(sheet: str, cell: str):
    sess.select_with_focus(sheet, cell)

    return {"code": 1, "message": "Success."}


@app.post("/prob", response_model=ProbRes, tags=["Params"])
async def prob(prob_req: ProbReq):
    if prob_req.dist in ['norm', 'normal', 'gauss', 'gaussian']:
        x, p = dists.stat_gen_dist_normal(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['exp', 'expon', 'exponential']:
        x, p = dists.stat_gen_dist_exponential(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['bet', 'beta']:
        x, p = dists.stat_gen_dist_beta(
            prob_req.start, prob_req.end, prob_req.step, prob_req.a, prob_req.b, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['uni', 'unif', 'uniform']:
        x, p = dists.stat_gen_dist_uniform(
            prob_req.start, prob_req.end, prob_req.step, prob_req.loc, prob_req.scale)
    elif prob_req.dist in ['bern', 'bernoulli']:
        x, p = dists.stat_gen_dist_bernoulli(
            prob_req.start, prob_req.end, prob_req.p, prob_req.loc)
    elif prob_req.dist in ['binom', 'binomial']:
        x, p = dists.stat_gen_dist_binom(
            prob_req.start, prob_req.end, prob_req.step, prob_req.p, prob_req.loc)
    elif prob_req.dist in ['poiss', 'poisson']:
        x, p = dists.stat_gen_dist_poisson(
            prob_req.start, prob_req.end, prob_req.step, prob_req.mu, prob_req.loc)
    else:
        raise NotImplementedError

    return {"dist": prob_req.dist,
            "x": x.tolist(),
            "prob": p.tolist(),
            "code": 1,
            "message": "Success: Variable processed with requested distribution."}


@app.post("/add_random_cell", response_model=Response, tags=["Params"])
async def add_random_cell(random_cell_add: RandomCellAdd):
    async with sess_lock:
        _key = '!'.join([random_cell_add.sheet, random_cell_add.cell])
        sess.random_cells[_key] = random_cell_add.x
        sess.probs[_key] = random_cell_add.prob
        sess.random_cells_aliases[_key] = dict({'alias': random_cell_add.alias, 'description': random_cell_add.description})

        sess.hash_params = sess.get_hash_params()

    return {"code": 1, "message": sess.hash_params}


@app.post("/remove_random_cell", response_model=Response, tags=["Params"])
async def remove_random_cell(random_cell_remove: RandomCellRemove):
    async with sess_lock:
        _key = '!'.join([random_cell_remove.sheet, random_cell_remove.cell])
        del sess.random_cells[_key]
        del sess.probs[_key]

        sess.hash_params = sess.get_hash_params()

    return {"code": 1, "message": sess.hash_params}


@app.post("/add_monitoring_cell", response_model=Response, tags=["Params"])
async def add_monitoring_cell(monitoring_cell_add: MonitoringCellReqs):
    async with sess_lock:
        _key = '!'.join([monitoring_cell_add.sheet, monitoring_cell_add.cell])
        sess.monitoring_cells[_key] = []
        sess.monitoring_cells_aliases[_key] = dict({'alias': monitoring_cell_add.alias, 'description': monitoring_cell_add.description})


        sess.hash_params = sess.get_hash_params()

    return {"code": 1, "message": sess.hash_params}


@app.post("/remove_monitoring_cell", response_model=Response, tags=["Params"])
async def remove_monitoring_cell(monitoring_cell_remove: MonitoringCellReqs):
    async with sess_lock:
        _key = '!'.join([monitoring_cell_remove.sheet, monitoring_cell_remove.cell])
        del sess.monitoring_cells[_key]

        sess.hash_params = sess.get_hash_params()

    return {"code": 1, "message": sess.hash_params}


@app.get("/check_connection", response_model=Response, tags=["Connection"])
async def check_connection():
    if sess.check_connection():
        return {"code": 1, "message": f"{sess.filename_ext}"}
    else:
        if sess.filename_ext:
            return {"code": 0, "message": f"Disconnected"}
        else:
            return {"code": -1, "message": f"Never connected"}


@app.post("/run_sim_start", response_model=Response, tags=["Simulation"])
async def run_sim_start(run_sim_start_req: RunSimStartReq):
    sess.workbook_obj.app.screen_updating = False
    sess.workbook_obj.app.calculation = 'manual'

    sess.saved = None
    sess.run_benchmark()

    # API calls: 0.8 times/sec(during run_sim_start), takes 50ms(during no run_sim_start) each.
    _async_sleep = .05
    _max_blocking = 1.25
    _safety_level = .95
    if sess.throughput:
        _num_chunk = max(round(sess.throughput * _max_blocking * _safety_level), 1)
    else:
        _num_chunk = 5

    print(_num_chunk)

    sess.task = asyncio.create_task(
        sess.run_simulation(async_sleep=_async_sleep, num_chunk=_num_chunk, num_trials=run_sim_start_req.num_trials))
    try:
        await sess.task
    except asyncio.CancelledError:
        print('Initial task cancelled.')

    sess.workbook_obj.app.screen_updating = True
    sess.workbook_obj.app.calculation = 'automatic'

    return {"code": 1, "message": f"Success"}


@app.get("/run_sim_cancel", response_model=Response, tags=["Simulation"])
async def run_sim_cancel():
    res = asyncio.create_task(sess.stop_simulation(cancel=True))
    await res

    sess.workbook_obj.app.screen_updating = True
    sess.workbook_obj.app.calculation = 'automatic'

    return {"code": 1, "message": f"Success"}


@app.get("/run_sim_pause", response_model=Response, tags=["Simulation"])
async def run_sim_pause():
    res = asyncio.create_task(sess.stop_simulation(cancel=False))
    await res

    sess.workbook_obj.app.screen_updating = True
    sess.workbook_obj.app.calculation = 'automatic'

    return {"code": 1, "message": f"Success"}


@app.get("/run_sim_resume", response_model=Response, tags=["Simulation"])
async def run_sim_resume():
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


@app.get("/run_sim_progress", response_model=Progress, tags=["Simulation"])
async def run_sim_progress():
    if sess.progress is None:
        return {"progress": None, "code": 0, "message": f"Failed: Not even 0%."}
    else:
        return {"progress": sess.progress, "code": 1, "message": f"{sess.progress * 100}%."}


@app.post("/run_sim_preview", response_model=PreviewDataRes, tags=["Simulation"])
async def run_sim_preview(preview_data_req: PreviewDataReq):
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


@app.get("/run_sim_save", response_model=Response, tags=["Simulation"])
async def run_sim_save():
    ts_saved = time.time()

    # Parameters
    # Existence check
    stmt = exists().where(params_table.c.hash_params == sess.hash_params).select()

    async with engine.connect() as conn:
        _res_exists = await conn.execute(stmt)
        await conn.commit()

    # Proceed
    if not _res_exists.first()[0]:
        values = []
        _raw_params = {'r': sess.random_cells, 'p': sess.probs, 'm': sess.monitoring_cells}
        for t in _raw_params.keys():
            for k in _raw_params[t].keys():
                if t == 'm':
                    values.append((sess.hash_params, t, k, None, None))
                else:
                    for i, v in enumerate(_raw_params[t][k]):
                        values.append((sess.hash_params, t, k, i, v))

        stmt = insert(params_table).values(values)

        if values:
            async with engine.connect() as conn:
                _res_par = await conn.execute(stmt)
                await conn.commit()

            _sig_par = 1
        else:
            _sig_par = 0
    else:
        _sig_par = 1

    # Aliases
    # Existence check
    stmt = exists().where(
        (alias_table.c.hash_params == sess.hash_params)
    ).select()

    async with engine.connect() as conn:
        _res_exists = await conn.execute(stmt)
        await conn.commit()

    # Create
    if not _res_exists.first()[0]:
        values = []
        for k in sess.random_cells_aliases.keys():
            values.append((sess.hash_params, 'r', k, sess.random_cells_aliases[k]['alias'], sess.random_cells_aliases[k]['description']))
        for k in sess.monitoring_cells_aliases.keys():
            values.append((sess.hash_params, 'm', k, sess.monitoring_cells_aliases[k]['alias'], sess.monitoring_cells_aliases[k]['description']))
        
        stmt = insert(alias_table).values(values)

        if values:
            async with engine.connect() as conn:
                _res = await conn.execute(stmt)
                await conn.commit()

            _sig_ali = 1
        else:
            _sig_ali = 0
    else:
        _sig_ali = 1

    # Records
    if sess.saved:
        first_n = sess.saved + 1
    else:
        first_n = 0

    last_n = min([len(v) for v in sess.monitoring_cells.values()])
    values = []
    for n in range(first_n, last_n):
        _hash_records = sess.get_hash_records(ts_saved=ts_saved, loop=n)
        for k in sess.monitoring_cells.keys():
            values.append((sess.filename_ext, sess.hash_params, ts_saved, 'm', k, n, _hash_records, sess.monitoring_cells[k][n]))

        for k in sess.trial_cells.keys():
            values.append((sess.filename_ext, sess.hash_params, ts_saved, 't', k, n, _hash_records, sess.trial_cells[k][n]))

        sess.saved = n

    if values:
        stmt = insert(snapshots_table).values(values)
        async with engine.connect() as conn:
            _res_rec = await conn.execute(stmt)
            await conn.commit()

        _sig_rec = 1
    else:
        _sig_rec = 0

    return {"code": _sig_rec and _sig_par and _sig_ali, "message": f"Rec: {_sig_rec} / Par: {_sig_par} / Ali: {_sig_ali}"}

  
@app.get("/get_hist_list", response_model=list[HistListRes], tags=["History"])
async def get_hist_list(offset: int = 0, limit: int = 100):
    stmt = select(
        snapshots_table.c.filename,
        snapshots_table.c.hash_params,
        snapshots_table.c.saved,
        func.count().label("samples")
    ).distinct().group_by(
        snapshots_table.c.hash_params,
        snapshots_table.c.saved,
        snapshots_table.c.cell_type,
        snapshots_table.c.cell_address,
    ).offset(offset).limit(limit)

    async with engine.connect() as conn:
        res = await conn.execute(stmt)

    return res.fetchall()


@app.get("/get_hist_list_params", response_model=list[HistListParamsRes], tags=["History"])
async def get_hist_list_params(offset: int = 0, limit: int = 100):
    # SQLAlchemy not supporting View: https://stackoverflow.com/a/9769411/3054161
    # Nested sub-queries vs View performance will be the same: https://stackoverflow.com/a/25603457/3054161

    stmt_distinct = select(
        snapshots_table.c.filename,
        snapshots_table.c.hash_params,
        snapshots_table.c.cell_type,
        snapshots_table.c.cell_address,
    ).distinct().subquery()

    stmt_count = select(
        stmt_distinct.c.filename,
        stmt_distinct.c.hash_params,
        stmt_distinct.c.cell_type,
        func.count(
            stmt_distinct.c.cell_address,
        ).label("cell_address")
    ).group_by(
        stmt_distinct.c.filename,
        stmt_distinct.c.hash_params,
        stmt_distinct.c.cell_type,
    ).subquery()

    stmt_rand = select(
        stmt_count.c.filename,
        stmt_count.c.hash_params,
        stmt_count.c.cell_address.label('random'),
    ).where(stmt_count.c.cell_type == "t").subquery()

    stmt_monit = select(
        stmt_count.c.filename,
        stmt_count.c.hash_params,
        stmt_count.c.cell_address.label('monitoring'),
    ).where(stmt_count.c.cell_type == "m").subquery()

    stmt_join = select(
        stmt_rand.c.filename,
        stmt_rand.c.hash_params,
        stmt_rand.c.random,
        stmt_monit.c.monitoring,
    ).select_from(
        stmt_rand.join(
            stmt_monit,
            onclause=(stmt_rand.c.hash_params == stmt_monit.c.hash_params),
            isouter=True
        )
    ).offset(offset).limit(limit)

    async with engine.connect() as conn:
        res = await conn.execute(stmt_join)
        await conn.commit()

    return res.fetchall()


@app.get("/get_hist_sim_csv", response_class=StreamingResponse, tags=["History"])
async def get_hist_sim_csv(hash_params: str):
    stmt = select(
        snapshots_table.c.hash_records,
        snapshots_table.c.cell_type,
        snapshots_table.c.cell_address,
        snapshots_table.c.cell_value,
    ).where(
        snapshots_table.c.hash_params == hash_params
    )

    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    # Rec to df.
    df = DataFrame(res.fetchall()).pivot(index=['hash_records'], columns=['cell_type', 'cell_address'], values=['cell_value']).reset_index()
    df.columns = [df.columns.values[0][0]] + [f"{col[1].upper()}: {col[2]}" for col in df.columns.values[1:]]

    return StreamingResponse(io.StringIO(df.to_csv(index=False)), media_type="text/csv")


@app.post("/del_hist_sim", response_model=Response, tags=["History"])
async def del_hist_sim(del_hist_sim_req: DelHistSimReq):
    stmt = delete(snapshots_table)\
        .where(snapshots_table.c.filename == del_hist_sim_req.filename)\
        .where(snapshots_table.c.hash_params == del_hist_sim_req.hash_params)

    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    return {"code": 1, "message": f"Success({res.rowcount})"}


@app.get("/get_hist_sim_corr", response_model=list[HistSimCorrRes], tags=["History"])
async def get_hist_sim_corr(hash_params: str):
    stmt = select(
        snapshots_table.c.cell_type,
        snapshots_table.c.cell_address,
        snapshots_table.c.hash_records,
        snapshots_table.c.cell_value,
    ).where(
        snapshots_table.c.hash_params == hash_params
    )

    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    # Rec to df.
    df = DataFrame(res.fetchall()).pivot(index=['hash_records'], columns=['cell_type', 'cell_address'], values=['cell_value']).reset_index()
    df.columns = [df.columns.values[0][0]] + [f"{col[1].upper()}: {col[2]}" for col in df.columns.values[1:]]

    # Calc Corr.
    df_corr_recs = df.corr().unstack().reset_index()
    df_corr_recs.columns = ['x', 'y', 'v']

    return df_corr_recs.to_dict(orient='records')


@app.post("/get_hist_sim_summary", response_model=list[HistSimSummaryRes], tags=["History"])
async def get_hist_sim_summary(hist_sim_summary_req: HistSimSummaryReq):
    stmt = select(
        snapshots_table.c.hash_records,
        snapshots_table.c.cell_type,
        snapshots_table.c.cell_address,
        snapshots_table.c.cell_value,
    ).where(
        snapshots_table.c.hash_params == hist_sim_summary_req.hash_params
    )

    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    # Rec to df.
    df = DataFrame(res.fetchall()).pivot(index=['hash_records'], columns=['cell_type', 'cell_address'], values=['cell_value']).reset_index()
    df.columns = [df.columns.values[0][0]] + [f"{col[1].upper()}: {col[2]}" for col in df.columns.values[1:]]

    df_summary = df.describe().unstack().reset_index()
    df_summary.columns = ['column', 'stats', 'value']

    return df_summary.to_dict(orient='records')


@app.get("/get_hist_sim_params", response_model=list[HistSimParamsRes], tags=["History"])
async def get_hist_sim_params(hash_params: str):
    stmt = select(
        params_table.c.param_type,
        params_table.c.cell_address,
        params_table.c.param_index,
        params_table.c.param_value,
    ).where(
        params_table.c.hash_params == hash_params
    )

    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    return res.fetchall()


@app.post("/get_hist_sim_recs", tags=["History"])#, response_model=list[SimDataRes])
async def get_hist_sim_recs(hist_sim_recs_req: HistSimRecsReq):
    stmt = select(
        snapshots_table.c.hash_records,
        snapshots_table.c.cell_type,
        snapshots_table.c.cell_address,
        snapshots_table.c.cell_value,
    ).where(snapshots_table.c.hash_params == hist_sim_recs_req.hash_params)

    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    # Rec to df.
    df = DataFrame(res.fetchall()).pivot(index=['hash_records'], columns=['cell_type', 'cell_address'], values=['cell_value']).reset_index()
    df.columns = [df.columns.values[0][0]] + [f"{col[1].upper()}: {col[2]}" for col in df.columns.values[1:]]

    return df.drop('hash_records', axis=1).to_dict(orient='records')


@app.post("/set_alias", tags=["Alias"], response_model=Response, deprecated=True)
async def set_alias(alias: Alias):
    # Aliases
    # Existence check
    stmt = exists().where(
        (alias_table.c.hash_params == alias.hash_params)
         & (alias_table.c.cell_address == alias.cell_address)
    ).select()

    async with engine.connect() as conn:
        _res_exists = await conn.execute(stmt)
        await conn.commit()

    # Create or Update
    if not _res_exists.first()[0]:
        stmt = insert(alias_table).values(
            hash_params=alias.hash_params,
            cell_address=alias.cell_address,
            cell_alias=alias.cell_alias,
            cell_description=alias.cell_description,
        )
    else:
        stmt = update(alias_table).where(
            (alias_table.c.hash_params == alias.hash_params)
            & (alias_table.c.cell_address == alias.cell_address)
        ).values(
            hash_params=alias.hash_params,
            cell_address=alias.cell_address,
            cell_alias=alias.cell_alias,
            cell_description=alias.cell_description,
        )

    async with engine.connect() as conn:
        _res = await conn.execute(stmt)
        await conn.commit()

    return {"code": 1, "message": "Success: Alias saved."}


@app.get("/get_alias", tags=["Alias"], response_model=list[Alias])
async def get_alias(hash_params: str):
    stmt = select(
        alias_table.c.hash_params,
        alias_table.c.cell_type,
        alias_table.c.cell_address,
        alias_table.c.cell_alias,
        alias_table.c.cell_description,
    ).where(
        alias_table.c.hash_params == hash_params
    )
    
    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    return res.fetchall()


@app.post("/del_alias", tags=["Alias"], response_model=Response)
async def del_alias(hash_params: str):
    stmt = delete(alias_table).where(
        alias_table.c.hash_params == hash_params
    )
    
    async with engine.connect() as conn:
        res = await conn.execute(stmt)
        await conn.commit()

    return {"code": 1, "message": f"Success({res.rowcount})"}
