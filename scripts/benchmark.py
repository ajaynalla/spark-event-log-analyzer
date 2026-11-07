"""Resource check including response encoding, excluding network and HTTP overhead."""
import ctypes
import json
import sys
import time
from tempfile import TemporaryFile
from spark_perf_analyzer.web_analysis import analyze_stream

started=time.perf_counter()
with TemporaryFile('w+b') as f:
    for i in range(25000):
        event={'Event':'SparkListenerTaskEnd','Stage ID':0,'Task Info':{'Task ID':i,'Index':i,'Attempt':0,'Executor ID':str(i%4),'Launch Time':1000,'Finish Time':1100},'Task End Reason':{'Reason':'Success'},'Task Metrics':{'Executor Run Time':100,'Input Metrics':{'Bytes Read':i},'Shuffle Read Metrics':{'Remote Bytes Read':i,'Local Bytes Read':5},'Shuffle Write Metrics':{'Shuffle Bytes Written':10}}}
        f.write(json.dumps(event,separators=(',',':')).encode()+b'\n')
    size=f.tell();f.seek(0)
    report=analyze_stream(f)
    response=json.dumps(report,separators=(',',':')).encode()
assert report['summary']['tasks']==25000
if sys.platform=='win32':
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_=[('cb',wintypes.DWORD),('PageFaultCount',wintypes.DWORD)]+[(n,ctypes.c_size_t) for n in ('PeakWorkingSetSize','WorkingSetSize','QuotaPeakPagedPoolUsage','QuotaPagedPoolUsage','QuotaPeakNonPagedPoolUsage','QuotaNonPagedPoolUsage','PagefileUsage','PeakPagefileUsage')]
    counters=Counters();counters.cb=ctypes.sizeof(counters)
    ctypes.windll.kernel32.GetCurrentProcess.restype=wintypes.HANDLE
    ctypes.windll.psapi.GetProcessMemoryInfo.argtypes=[wintypes.HANDLE,ctypes.POINTER(Counters),wintypes.DWORD]
    if not ctypes.windll.psapi.GetProcessMemoryInfo(ctypes.windll.kernel32.GetCurrentProcess(),ctypes.byref(counters),counters.cb):
        raise ctypes.WinError()
    peak=counters.PeakWorkingSetSize
else:
    import resource
    peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
print(json.dumps({'tasks':25000,'input_bytes':size,'response_bytes':len(response),'analysis_ms':report['processing_ms'],'total_ms':round((time.perf_counter()-started)*1000,1),'peak_process_rss_mib':round(peak/1024**2,1),'platform':sys.platform,'python':sys.version.split()[0]},indent=2))
