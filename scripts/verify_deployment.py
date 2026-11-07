"""Public HTTP checks with disposable fixtures; never uploads user logs."""
import argparse
import json
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

parser=argparse.ArgumentParser()
parser.add_argument('--frontend',required=True)
parser.add_argument('--backend',required=True)
parser.add_argument('--output',type=Path)
parser.add_argument('--large',action='store_true')
args=parser.parse_args()
base=args.backend.rstrip('/')
root=Path(__file__).resolve().parents[1]
results=[]

def request(name,path,method='GET',body=None,headers=None,expected=200):
    started=time.perf_counter()
    req=Request(path if path.startswith('http') else base+path,data=body,method=method,headers=headers or {})
    try:
        with urlopen(req,timeout=180) as r:status,data,h=r.status,r.read(),dict(r.headers)
    except HTTPError as e:status,data,h=e.code,e.read(),dict(e.headers)
    assert status==expected,(name,status,data[:200])
    results.append({'check':name,'status':status,'elapsed_ms':round((time.perf_counter()-started)*1000,1)})
    return data,{k.lower():v for k,v in h.items()}

assert json.loads(request('health','/health')[0])['status']=='ok'
assert json.loads(request('limits','/api/config')[0])['max_upload_bytes']==25*1024**2
sample=json.loads(request('sample analysis','/api/sample')[0])
assert sample['summary']['tasks']==26 and sample['summary']['shuffle_read_bytes']==914358272
payload=request('sample download','/api/sample/download')[0]
uploaded=json.loads(request('sample upload','/api/analyze','POST',payload,{'Content-Type':'application/octet-stream'})[0])
assert uploaded['summary']==sample['summary']
fixture=(root/'tests/fixtures/apache-spark-event-log.jsonl').read_bytes()
upstream=json.loads(request('Apache Spark fixture upload','/api/analyze','POST',fixture,{'Content-Type':'application/octet-stream'})[0])
assert upstream['summary']['tasks']==6 and upstream['summary']['duration_ms']==62168
request('invalid log','/api/analyze','POST',b'not-json',{'Content-Type':'application/octet-stream'},422)
request('unsupported multipart','/api/analyze','POST',b'{}',{'Content-Type':'multipart/form-data'},415)
_,cors=request('frontend CORS','/api/analyze','OPTIONS',headers={'Origin':args.frontend.rstrip('/'),'Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'content-type'})
assert cors.get('access-control-allow-origin')==args.frontend.rstrip('/')
html,_=request('frontend HTML',args.frontend)
assert b'SparkLens' in html and b'type="module"' in html
if args.large:
    events=[]
    for i in range(25000):
        events.append(json.dumps({'Event':'SparkListenerTaskEnd','Stage ID':0,'Stage Attempt ID':0,'Task Info':{'Task ID':i,'Index':i,'Attempt':0,'Executor ID':str(i%4),'Launch Time':1000,'Finish Time':1100},'Task End Reason':{'Reason':'Success'},'Task Metrics':{'Executor Run Time':100,'Input Metrics':{'Bytes Read':i},'Shuffle Read Metrics':{'Remote Bytes Read':i,'Local Bytes Read':5},'Shuffle Write Metrics':{'Shuffle Bytes Written':10}}},separators=(',',':')))
    body=('\n'.join(events)+'\n').encode()
    report=json.loads(request('25,000-task supported boundary','/api/analyze','POST',body,{'Content-Type':'application/octet-stream'})[0])
    assert report['summary']['tasks']==25000 and report['summary']['shuffle_read_bytes']==sum(range(25000))+125000
    results[-1].update(upload_bytes=len(body),server_processing_ms=report['processing_ms'])
output={'verified_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'frontend':args.frontend,'backend':args.backend,'checks':results,'sample_summary':sample['summary'],'upstream_summary':upstream['summary']}
if args.output:
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(output,indent=2)+'\n',encoding='utf-8')
print(json.dumps(output,indent=2))
