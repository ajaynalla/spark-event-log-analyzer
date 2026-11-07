import type { Report } from './types'
export const base = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/,'')
export const LIMIT = 25 * 1024 * 1024
export function errorMessage(status:number, body?:string) {
  try { const data=JSON.parse(body || '{}'); if(data.error?.message) return data.error.message } catch { /* Render may serve HTML while waking. */ }
  return status === 0 ? 'Cannot reach the API. Check your connection and try again.' : status === 429 ? 'The analyzer is busy. Try again in a moment.' : `API returned ${status}. The free service may be waking up; retry in about a minute.`
}
export function analyze(file:File|null, progress:(n:number)=>void):Promise<Report> {
  return new Promise((resolve,reject)=>{
    if(file && file.size>LIMIT) { reject(new Error('File exceeds the 25 MiB upload limit.')); return }
    const xhr=new XMLHttpRequest()
    xhr.open(file?'POST':'GET',`${base}/api/${file?'analyze':'sample'}`)
    xhr.timeout=180000
    if(file) xhr.setRequestHeader('Content-Type','application/octet-stream')
    xhr.upload.onprogress=e=>{if(e.lengthComputable) progress(Math.round(e.loaded/e.total*100))}
    xhr.onerror=()=>reject(new Error(errorMessage(0)))
    xhr.ontimeout=()=>reject(new Error('The API timed out. The service may be waking; retry or use a smaller log.'))
    xhr.onload=()=>{
      if(xhr.status<200||xhr.status>=300) {reject(new Error(errorMessage(xhr.status,xhr.responseText)));return}
      try { const data=JSON.parse(xhr.responseText); if(data.schema_version!==1||!Array.isArray(data.tasks)) throw new Error(); resolve(data) }
      catch {reject(new Error('The API returned an unexpected response. Try again after the service wakes.'))}
    }
    xhr.send(file)
  })
}
