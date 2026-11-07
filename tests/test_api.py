import asyncio
import io
import pytest
from fastapi.testclient import TestClient
from spark_perf_analyzer import api
from spark_perf_analyzer.web_analysis import LogError

client=TestClient(api.app)


def test_health_config_and_request_headers():
    r=client.get('/health')
    assert r.status_code==200 and r.json()['status']=='ok'
    assert r.headers['x-request-id']
    assert r.headers['cache-control']=='no-store'
    assert client.get('/api/config').json()['max_upload_bytes']==25*1024**2


def test_sample_download_and_raw_upload_match():
    sample=client.get('/api/sample').json()
    raw=client.get('/api/sample/download')
    r=client.post('/api/analyze',content=raw.content,headers={'Content-Type':'application/octet-stream'})
    assert r.status_code==200
    assert r.json()['summary']==sample['summary']
    assert r.json()['source']=='uploaded event log'


@pytest.mark.parametrize('body',[b'',b'not-json',b'[]',b'\x1f\x8b'])
def test_invalid_upload_returns_structured_error(body):
    r=client.post('/api/analyze',content=body,headers={'Content-Type':'application/octet-stream'})
    assert r.status_code==422
    assert r.json()['error']['code']=='invalid_log'


def test_size_type_and_encoding_rejection():
    assert client.post('/api/analyze',content=b'{}',headers={'Content-Type':'application/octet-stream','Content-Length':str(25*1024**2+1)}).status_code==413
    assert client.post('/api/analyze',content=b'{}',headers={'Content-Type':'multipart/form-data'}).status_code==415
    assert client.post('/api/analyze',content=b'{}',headers={'Content-Type':'application/octet-stream','Content-Encoding':'gzip'}).status_code==415


def test_cors_allowlist():
    headers={'Origin':'http://localhost:5173','Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'content-type'}
    assert client.options('/api/analyze',headers=headers).headers['access-control-allow-origin']=='http://localhost:5173'
    headers['Origin']='https://untrusted.example'
    assert 'access-control-allow-origin' not in client.options('/api/analyze',headers=headers).headers


def test_busy_slot_rejected_and_cleanup_after_parse_error(monkeypatch):
    monkeypatch.setattr(api,'busy',True)
    assert client.get('/api/sample').status_code==429
    assert client.post('/api/analyze',content=b'{}').status_code==429
    monkeypatch.setattr(api,'busy',False)
    handles=[]
    original=api.TemporaryFile
    def capture(*a,**kw):
        f=original(*a,**kw);handles.append(f);return f
    monkeypatch.setattr(api,'TemporaryFile',capture)
    client.post('/api/analyze',content=b'bad',headers={'Content-Type':'application/octet-stream'})
    assert handles[0].closed and not api.busy


def test_upload_timeout_releases_slot(monkeypatch):
    async def timeout(coro,timeout):
        coro.close()
        raise asyncio.TimeoutError()
    monkeypatch.setattr(api.asyncio,'wait_for',timeout)
    r=client.post('/api/analyze',content=b'{}',headers={'Content-Type':'application/octet-stream'})
    assert r.status_code==408 and r.json()['error']['code']=='upload_timeout'
    assert not api.busy
