import io
import json
import time
import pytest

from spark_perf_analyzer import web_analysis as wa
from spark_perf_analyzer.api import SAMPLE


def task(tid=1, runtime=100, partition=0, attempt=0, stage_attempt=0, reason="Success", **extra):
    return {"Event":"SparkListenerTaskEnd", "Stage ID":0, "Stage Attempt ID":stage_attempt,
            "Task Info":{"Task ID":tid,"Index":partition,"Attempt":attempt,"Launch Time":1000,"Finish Time":1100,"Executor ID":"1"},
            "Task End Reason":{"Reason":reason}, "Task Metrics":{"Executor Run Time":runtime,
                "Input Metrics":{"Bytes Read":extra.get("load",10)},
                "Shuffle Read Metrics":{"Remote Bytes Read":extra.get("remote",20),"Local Bytes Read":5},
                "Shuffle Write Metrics":{"Shuffle Bytes Written":30}}}


def analyze(*events):
    return wa.analyze_stream(io.BytesIO(b"\n".join(json.dumps(e).encode() for e in events)))


def test_demo_exact_metrics():
    with SAMPLE.open("rb") as f:
        r=wa.analyze_stream(f,sample=True)
    s=r["summary"]
    assert (s["jobs"],s["stages"],s["tasks"],s["completed_tasks"],s["failed_tasks"]) == (2,3,26,25,1)
    assert s["duration_ms"] == 22000
    assert s["shuffle_read_bytes"] == 872*1024**2
    assert s["shuffle_write_bytes"] == 920*1024**2
    assert s["stragglers"] == 1
    assert s["skewed_stages"] == 1
    assert r["stages"][1]["retried_tasks"] == 1
    assert r["source"] == "synthetic sample"


def test_elapsed_is_not_summed_task_time_and_bytes_are_remote_plus_local():
    r=analyze(task(),task(2,partition=1))
    assert r["stages"][0]["duration_ms"] == 100
    assert r["stages"][0]["task_time_ms"] == 200
    assert r["summary"]["shuffle_read_bytes"] == 50
    assert r["summary"]["completed_tasks"] == 2


def test_success_baseline_excludes_failed_retry_and_uses_partition_identity():
    r=analyze(task(1,10000,0,reason="FetchFailed"),task(2,100,0,attempt=1),task(3,100,1),task(4,100,2),task(5,1000,3))
    stage=r["stages"][0]
    assert stage["runtime_avg_ms"] == 325
    assert stage["runtime_skew_ratio"] == 1000/325
    assert stage["retried_tasks"] == 1
    assert r["summary"]["stragglers"] == 1


def test_partition_imbalance_includes_measured_zero_loads():
    ts=[task(i,partition=i,load=0,remote=0) for i in range(4)]
    for t in ts: t["Task Metrics"]["Shuffle Read Metrics"]["Local Bytes Read"]=0
    ts[-1]["Task Metrics"]["Input Metrics"]["Bytes Read"]=100
    assert analyze(*ts)["stages"][0]["partition_imbalance_ratio"] == 4


def test_stage_attempts_and_task_start_end_deduplicated():
    end=task()
    begin={**end,"Event":"SparkListenerTaskStart"}
    r=analyze(begin,end,end,task(1,stage_attempt=1))
    assert r["summary"]["tasks"] == 2
    assert r["summary"]["stages"] == 1
    assert r["summary"]["stage_attempts"] == 2


def test_missing_metrics_are_null_and_unknown_is_not_failed():
    t=task(); t["Task Metrics"]={};t.pop("Task End Reason");t["Task Info"].pop("Launch Time");t["Task Info"].pop("Finish Time")
    r=analyze(t)
    assert r["tasks"][0]["runtime_ms"] is None
    assert r["tasks"][0]["shuffle_read_bytes"] is None
    assert r["stages"][0]["runtime_skew_ratio"] is None
    assert r["summary"]["duration_ms"] is None
    assert r["summary"]["failed_tasks"] == 0
    assert not r["diagnostics"]
    assert r["warnings"]


@pytest.mark.parametrize("raw",[b'',b'{}',b'[]',b'null',b'not json',b'\xff',b'{"Event":"Other"}',b'{"Event":"SparkListenerApplicationStart"}'])
def test_invalid_or_unusable_logs(raw):
    with pytest.raises(wa.LogError):wa.analyze_stream(io.BytesIO(raw))


@pytest.mark.parametrize("field,value",[("Stage ID",-1),("Stage ID",True),("Stage ID",1.5),("Stage ID",2**60)])
def test_invalid_numeric_values(field,value):
    t=task();t[field]=value
    with pytest.raises(wa.LogError):analyze(t)


def test_invalid_nested_metrics():
    t=task();t["Task Metrics"]=[]
    with pytest.raises(wa.LogError):analyze(t)


def test_limits_and_deadline(monkeypatch):
    monkeypatch.setattr(wa,"MAX_BYTES",10)
    with pytest.raises(wa.LogError,match="25 MiB"):analyze(task())
    monkeypatch.setattr(wa,"MAX_BYTES",25*1024**2)
    monkeypatch.setattr(wa,"MAX_TASKS",1)
    with pytest.raises(wa.LogError,match="25,000"):analyze(task(1),task(2))
    with pytest.raises(wa.LogError,match="exceeded 30"):
        wa.read_events(io.BytesIO(b'{}'), time.monotonic()-1)


def test_large_supported_log_10000_tasks():
    r=analyze(*(task(i,partition=i,remote=i) for i in range(10000)))
    assert r["summary"]["tasks"] == 10000
    assert r["summary"]["shuffle_read_bytes"] == sum(range(10000))+50000
    assert r["summary"]["completed_tasks"] == 10000


def test_unknown_event_and_sensitive_properties_not_returned():
    r=analyze({"Event":"SparkListenerEnvironmentUpdate","Spark Properties":{"spark.app.id":"app-test","password":"do-not-return"}},task())
    assert r["application"]["id"] == "app-test"
    assert "do-not-return" not in json.dumps(r)


def test_upstream_apache_spark_fixture():
    with open('tests/fixtures/apache-spark-event-log.jsonl','rb') as handle:
        r=wa.analyze_stream(handle)
    assert r['event_count']==44
    assert r['summary']['jobs']==3
    assert r['summary']['stages']==3
    assert r['summary']['tasks']==6
    assert r['summary']['completed_tasks']==6
    assert r['summary']['duration_ms']==62168
    assert r['summary']['shuffle_read_bytes']==0
    assert r['summary']['failed_tasks']==0


def test_multiple_applications_rejected():
    with pytest.raises(wa.LogError,match='one Spark application'):
        analyze({'Event':'SparkListenerApplicationStart'},{'Event':'SparkListenerApplicationStart'},task())


def test_reversed_task_timestamps_rejected():
    t=task();t['Task Info']['Finish Time']=10
    with pytest.raises(wa.LogError,match='precedes'):
        analyze(t)


def test_taskstart_zero_finish_is_unknown_not_reversed():
    t=task();t['Event']='SparkListenerTaskStart';t['Task Info']['Finish Time']=0
    r=analyze(t)
    assert r['tasks'][0]['end_ms'] is None
    assert r['tasks'][0]['status']=='incomplete'
