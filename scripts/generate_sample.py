"""Reproducible synthetic Spark-shaped events, not a captured production run."""
import json
from pathlib import Path

events = []
def add(kind, **fields):
    events.append({"Event": "SparkListener" + kind, **fields})

start = 1776772800000
add("LogStart", **{"Spark Version": "3.5.0"})
add("ApplicationStart", **{"App Name": "Retail orders · daily aggregation (synthetic)", "App ID": "demo-retail-orders", "Timestamp": start})
task_id = 0
for stage, name, offset, runs, loads, parents in [
    (0, "Scan & filter orders", 1000, [700, 820, 760, 900, 850, 790, 810, 840], [16]*8, []),
    (1, "SortMergeJoin · customer_id", 3500, [1200, 1350, 1100, 1250, 1400, 1300, 1150, 14500], [24]*7+[600], [0]),
    (2, "Aggregate & write daily sales", 19000, [900, 1050, 950, 1100, 1000, 980, 970, 1200], [10]*8, [1]),
]:
    job_id = 0 if stage < 2 else 1
    if stage in (0, 2):
        add("JobStart", **{"Job ID": job_id, "Stage IDs": [0, 1] if stage == 0 else [2], "Submission Time": start+offset-100})
    si = {"Stage ID": stage, "Stage Attempt ID": 0, "Stage Name": name, "Number of Tasks": len(runs), "Parent IDs": parents, "Submission Time": start+offset}
    add("StageSubmitted", **{"Stage Info": si})
    for index, (runtime, load) in enumerate(zip(runs, loads)):
        launch = start+offset+index*30
        info = {"Task ID": task_id, "Index": index, "Attempt": 0, "Executor ID": str(index%3+1), "Launch Time": launch, "Finish Time": launch+runtime+40}
        metrics = {"Executor Run Time": runtime, "JVM GC Time": runtime//30,
                   "Input Metrics": {"Bytes Read": load*1024**2 if stage==0 else 0},
                   "Shuffle Read Metrics": {"Remote Bytes Read": load*1024**2 if stage>0 else 0, "Local Bytes Read": 0, "Fetch Wait Time": runtime//10 if stage==1 else 0},
                   "Shuffle Write Metrics": {"Shuffle Bytes Written": load*1024**2 if stage<2 else 0}}
        add("TaskEnd", **{"Stage ID": stage, "Stage Attempt ID": 0, "Task Info": info, "Task Metrics": metrics, "Task End Reason": {"Reason": "Success"}})
        task_id += 1
    if stage == 1:
        # A failed physical attempt, then a successful retry on the same partition.
        for attempt, outcome in ((0,"FetchFailed"),(1,"Success")):
            info={"Task ID":task_id,"Index":8,"Attempt":attempt,"Executor ID":"2","Launch Time":start+offset+200+attempt*1400,"Finish Time":start+offset+1200+attempt*1400}
            add("TaskEnd", **{"Stage ID":stage,"Stage Attempt ID":0,"Task Info":info,"Task Metrics":{"Executor Run Time":950,"Input Metrics":{"Bytes Read":0},"Shuffle Read Metrics":{"Remote Bytes Read":12*1024**2,"Local Bytes Read":0,"Fetch Wait Time":150},"Shuffle Write Metrics":{"Shuffle Bytes Written":12*1024**2}},"Task End Reason":{"Reason":outcome}})
            task_id+=1
        si["Number of Tasks"]=9
    si={**si,"Completion Time":start+offset+max(runs)+500}
    add("StageCompleted", **{"Stage Info":si})
    if stage in (1,2):
        add("JobEnd", **{"Job ID":job_id,"Completion Time":si["Completion Time"]+50,"Job Result":{"Result":"JobSucceeded"}})
add("ApplicationEnd", **{"Timestamp":start+22000})
path = Path(__file__).resolve().parents[1]/"src/spark_perf_analyzer/data/demo.jsonl"
path.parent.mkdir(parents=True,exist_ok=True)
path.write_text("\n".join(json.dumps(e,separators=(',',':')) for e in events)+"\n",encoding="utf-8")
print(f"Wrote {len(events)} explicitly synthetic events")
