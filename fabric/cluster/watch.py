from pathlib import Path
import json
import psycopg

from pydantic import BaseModel, validator


class Record(BaseModel):
    name: str
    todo: int
    meter: str = ""
    beat: str = ""
    caller: str = ""
    done: bool = False
    elapsed: int = 0
    path: str = ""
    sbatch: str = ""

    @validator("todo")
    def check_todo(cls, todo):
        # 1 for smit, 2 for cancel
        assert todo in (0, 1, 2)
        return todo


def open_db():
    with Path("~/psql_cred").expanduser().open("r") as f:
        cred = f.read()
    conn = psycopg.connect(cred)
    return conn


class ConnWrapper():
    """
    these are just some pythonic shortcuts
    sql itself is inconvenient
    """
    def __init__(self, conn):
        self.conn = conn

    def get_all_jobs(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs")
            res = cur.fetchall()
        records = bind_records(res)
        return records

    def insert(self, records):
        with self.conn.cursor() as cur:
            for row in records:
                data = row.dict()
                keys = tuple(data.keys())
                key_fields = str(keys).replace('\'', "\"")  # terrible
                vals = list(data.values())
                template = f"""
                    INSERT INTO "public"."jobs"
                    {key_fields}
                    VALUES
                    ({("%s, " * len(vals))[:-2]});
                """  # -2 is a terrible hack to remove the last comma... I don't like sql
                cur.execute(template, vals)

    def update(self, name, field, new_val):
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE "public"."jobs" SET "{field}" = %s WHERE "name" = %s;
                """,
                [new_val, name]
            )

    def drop_row(self, name):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM "public"."jobs" WHERE "name" = %s;
                """,
                [name]
            )

    def commit(self):
        return self.conn.commit()


def bind_records(jobs):
    # ugly sql hack
    fields = list(Record.__fields__.keys())
    records = []
    for j in jobs:
        j = j[1:]
        kwargs = {k: v for (k, v) in zip(fields, j)}
        r = Record(**kwargs)
        records.append(r)
    return records


def survey(interval=5, drop_on_done=False):
    from time import sleep
    from datetime import timedelta

    with open_db() as conn:
        conn = ConnWrapper(conn)
        while True:
            print("surverying")
            records = conn.get_all_jobs()
            for r in records:
                jname = r.name
                hbeat = Path(r.path) / "heartbeat.json"
                if hbeat.is_file():
                    with hbeat.open("r") as f:
                        info = json.load(f)

                    for k in info.keys():
                        conn.update(jname, k, info[k])

                    if info['done']:
                        if drop_on_done:
                            conn.drop_row(jname)
                        continue

                    if timedelta(seconds=info['elapsed']) > timedelta(hours=3, minutes=50):
                        conn.update(jname, "todo", 1)

            conn.commit()
            sleep(interval)
