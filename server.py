#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NotebookLM 去水印 · 网页版后端。

浏览器只负责交互，算法（unwatermark.clean_pdf）跑在本机。
服务只监听 127.0.0.1，PDF 不出这台电脑。

上传走 application/octet-stream 而不是表单，省掉 python-multipart 这个依赖。

启动：python3 server.py            （或双击 启动去水印网页.command）
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from collections import deque
from dataclasses import dataclass, field
from html import escape
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional
from urllib.parse import unquote

import fitz
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from PIL import Image
from starlette.background import BackgroundTask

from unpptx import PptxOptions, clean_pptx
from unwatermark import CleanOptions, clean_pdf
from decorate_pdf import FONTS, POSITIONS, add_logo, add_text

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
WORK_ROOT = Path(tempfile.gettempdir()) / "NotebookLMUnwatermark"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


# 公网部署时这些值全部可以用环境变量压低；本机自用保持默认即可。
MAX_UPLOAD_BYTES = _env_int("UNMARK_MAX_UPLOAD_MB", 50) * 1024 * 1024
"""单个上传的大小上限。公开服务保守默认 50 MB，降低内存与带宽峰值。"""

MAX_CONCURRENT_JOBS = _env_int("UNMARK_MAX_CONCURRENT", 1)
"""同时真正在跑的任务数。超出的排队等待——限制的是内存峰值，不是请求数。"""

MAX_LIVE_JOBS = _env_int("UNMARK_MAX_LIVE_JOBS", 12)
"""内存里同时保留的任务数（含已完成待下载的）。每个任务在磁盘上占两份 PDF。"""

RATE_LIMIT_JOBS = _env_int("UNMARK_RATE_LIMIT", 12)
"""同一来源在时间窗内最多能发起多少次任务。"""

RATE_LIMIT_WINDOW = timedelta(minutes=_env_int("UNMARK_RATE_WINDOW_MIN", 60))

JOB_TTL = timedelta(minutes=_env_int("UNMARK_JOB_TTL_MIN", 120))
"""任务连同临时文件的保留时长；到点清掉。"""

TRUST_PROXY = os.environ.get("UNMARK_TRUST_PROXY") == "1"
"""放在 Cloudflare / 负载均衡后面时置 1，才按 X-Forwarded-For 认来源 IP。"""

PUBLIC_MODE = os.environ.get("UNMARK_PUBLIC_MODE") == "1"
"""对外服务时置 1。页面上关于隐私的说法会跟着改——本机版说「不上传」，
公开版必须说清文件确实传到了服务器，以及多久删除。同一份 HTML 不能两边都吹。"""

RUN_SLOTS = threading.BoundedSemaphore(MAX_CONCURRENT_JOBS)


def render_page(path: Path, page_index: int, scale: float = 1.4) -> Image.Image:
    """把 PDF 的某一页渲染成图片，只读，不改文件。"""
    with fitz.open(path) as document:
        if page_index < 0 or page_index >= document.page_count:
            raise ValueError("页面编号超出范围。")
        pixmap = document[page_index].get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        return Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)


def render_slide(path: Path, page_index: int) -> Image.Image:
    """把整页位图型 PPTX 的某一页取出来，只读，不改文件。

    这种 PPTX 每页正好一张满版图，所以「渲染」就是把那张图从 zip 里读出来——
    不必也不能用 PDF 那套渲染器。
    """
    import zipfile

    from unpptx import bitmap_parts

    parts = bitmap_parts(path)
    if page_index < 0 or page_index >= len(parts):
        raise ValueError("页面编号超出范围。")
    with zipfile.ZipFile(path) as archive:
        return Image.open(BytesIO(archive.read(parts[page_index]))).convert("RGB")


@dataclass
class Job:
    """一次去水印任务的全部状态；只活在内存里，进程退出即消失。"""

    id: str
    name: str
    source: Path
    destination: Path
    kind: str = "pdf"                 # pdf / pptx
    status: str = "running"           # running / done / failed / cancelled
    stage: str = "正在读取文件…"
    current: int = 0
    total: int = 0
    result: Optional[dict] = None
    error: str = ""
    created: datetime = field(default_factory=datetime.now)
    cancelled: bool = False
    decorated: Optional[Path] = None

    @property
    def directory(self) -> Path:
        return self.source.parent

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "status": self.status,
            "stage": self.stage,
            "current": self.current,
            "total": self.total,
            "result": self.result,
            "error": self.error,
            "decorated": bool(self.decorated and self.decorated.exists()),
        }


JOBS: dict[str, Job] = {}
JOBS_LOCK = threading.Lock()

RATE_HITS: dict[str, deque] = {}
RATE_LOCK = threading.Lock()


def _client_key(request: Request) -> str:
    """限流用的来源标识。只有明确声明在反向代理后面时才信任转发头。"""
    if TRUST_PROXY:
        forwarded = request.headers.get("cf-connecting-ip") or request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(key: str) -> None:
    """滑动窗口限流；顺手把过期的来源整条丢掉，字典不会无限长大。"""
    now = time.monotonic()
    window = RATE_LIMIT_WINDOW.total_seconds()
    with RATE_LOCK:
        for stale_key in [k for k, hits in RATE_HITS.items() if not hits or now - hits[-1] > window]:
            if stale_key != key:
                RATE_HITS.pop(stale_key, None)
        hits = RATE_HITS.setdefault(key, deque())
        while hits and now - hits[0] > window:
            hits.popleft()
        if len(hits) >= RATE_LIMIT_JOBS:
            wait = int((window - (now - hits[0])) / 60) + 1
            raise HTTPException(
                status_code=429,
                detail=f"提交太频繁了，请约 {wait} 分钟后再试",
            )
        hits.append(now)


def _sweep_expired() -> None:
    """过期任务连同临时文件一起清掉，别让临时目录无限长大。"""
    deadline = datetime.now() - JOB_TTL
    with JOBS_LOCK:
        stale = [job for job in JOBS.values() if job.created < deadline and job.status != "running"]
        for job in stale:
            JOBS.pop(job.id, None)
    for job in stale:
        shutil.rmtree(job.directory, ignore_errors=True)


def _evict_overflow() -> None:
    """任务数超过上限时，从最旧的已结束任务开始丢，保护磁盘。"""
    with JOBS_LOCK:
        if len(JOBS) <= MAX_LIVE_JOBS:
            return
        finished = sorted(
            (job for job in JOBS.values() if job.status != "running"),
            key=lambda job: job.created,
        )
        dropped = finished[: len(JOBS) - MAX_LIVE_JOBS]
        for job in dropped:
            JOBS.pop(job.id, None)
    for job in dropped:
        shutil.rmtree(job.directory, ignore_errors=True)


def _run_job(job: Job, options, select: Optional[list[str]] = None) -> None:
    def progress(current: int, total: int, note: str) -> None:
        job.current, job.total, job.stage = current, total, note

    # 并发闸门：同时只让 MAX_CONCURRENT_JOBS 个任务真正开跑，其余在这里等。
    # 限的是内存峰值——每个任务解码整页位图，一起跑很容易把小内存实例打爆。
    if not RUN_SLOTS.acquire(blocking=False):
        job.stage = "排队中，等前面的任务跑完…"
        RUN_SLOTS.acquire()
    if job.cancelled:
        RUN_SLOTS.release()
        job.status, job.stage = "cancelled", "已取消"
        return
    try:
        if job.kind == "pptx":
            # PPTX 走的是完全不同的一条路：水印在那儿是个形状对象，直接删掉就行，
            # 无损、不重编码、也没有取消点可插（整件事快到不需要）。
            result = clean_pptx(job.source, job.destination, options,
                                select=select, progress=progress)
        else:
            result = clean_pdf(job.source, job.destination, options, progress,
                               lambda: job.cancelled)
        job.result = result.as_dict()
        if getattr(result, "cancelled", False):
            job.status, job.stage = "cancelled", "已取消"
        elif result.success:
            job.status, job.stage = "done", "完成"
        else:
            job.status, job.stage = "failed", "未处理"
            job.error = result.message
    except Exception as exc:  # pragma: no cover - 兜底，避免线程静默死掉
        job.status, job.stage, job.error = "failed", "出错", str(exc)
    finally:
        RUN_SLOTS.release()


def _get_job(job_id: str) -> Job:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在或已过期")
    return job


def _png(image: Image.Image, box_ratio: Optional[list] = None, pad: float = 1.2) -> Response:
    """按需把图裁到水印附近再输出，让「处理前/后」的差别一眼可见。

    裁剪范围随响应头回传：前端要把用户在放大图上框的位置换算回页面坐标，
    没有这个就只能猜，换算必错。
    """
    full_size = (image.width, image.height)
    crop = (0, 0, image.width, image.height)
    if box_ratio:
        x0, y0, x1, y1 = box_ratio
        width, height = image.width, image.height
        cx, cy = (x0 + x1) / 2 * width, (y0 + y1) / 2 * height
        half_w = max(60.0, (x1 - x0) * width * pad)
        half_h = max(24.0, (y1 - y0) * height * pad * 3)
        crop = (
            max(0, int(cx - half_w)), max(0, int(cy - half_h)),
            min(width, int(cx + half_w)), min(height, int(cy + half_h)),
        )
        image = image.crop(crop)
    buffer = BytesIO()
    image.save(buffer, "PNG")
    return Response(buffer.getvalue(), media_type="image/png",
                    headers={"Cache-Control": "no-store",
                             "X-Crop": ",".join(str(v) for v in crop),
                             "X-Full": f"{full_size[0]},{full_size[1]}",
                             "Access-Control-Expose-Headers": "X-Crop, X-Full"})


def _render_index() -> str:
    """把页面上关于隐私的说法按部署模式填进去。

    本机版「不上传任何服务器」是真的；公开部署后就不是了，所以那句话必须换掉，
    而不是两边都挂着。占位符写成 HTML 注释，直接打开文件也不会看到残留标记。

    页面是中英双语的，靠 data-zh / data-en 两个属性切换，所以这里填进去的
    也得是同一种形状——只填一种语言，切到另一种就会露出中文。
    """
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    limit_mb = MAX_UPLOAD_BYTES // (1024 * 1024)
    ttl_min = int(JOB_TTL.total_seconds() // 60)
    ttl_zh = f"{ttl_min // 60} 小时" if ttl_min >= 60 else f"{ttl_min} 分钟"
    ttl_en = (f"{ttl_min // 60} hour{'s' if ttl_min // 60 != 1 else ''}"
              if ttl_min >= 60 else f"{ttl_min} minutes")
    if PUBLIC_MODE:
        privacy_zh = (f"这是一个在线服务，文件会上传到服务器处理，单个最大 {limit_mb} MB。"
                      f"处理只在内存和临时目录里进行，原件与结果都在 {ttl_zh}后自动删除，"
                      "不做任何留存、不用于训练、也不会转给第三方。")
        privacy_en = (f"This is a hosted service: your file is uploaded to the server, "
                      f"up to {limit_mb} MB each. Processing happens in memory and a temporary "
                      f"directory; both the original and the result are deleted automatically "
                      f"after {ttl_en}. Nothing is retained, used for training, or passed on.")
        foot_zh = "文件会上传到服务器处理，用完即删。"
        foot_en = "Files are uploaded for processing and deleted afterwards."
    else:
        privacy_zh = ("现在跑的是本机版：服务只监听 127.0.0.1，文件不出这台电脑，"
                      "全程不联网。")
        privacy_en = ("You are running the local build: the service listens on 127.0.0.1 only, "
                      "the file never leaves this computer, and nothing goes online.")
        foot_zh = "本机运行，全程离线。"
        foot_en = "Running locally — fully offline."

    def pair(zh: str, en: str) -> str:
        return f'<span data-zh="{escape(zh, quote=True)}" data-en="{escape(en, quote=True)}"></span>'

    foot = pair(foot_zh, foot_en)
    return (html.replace("<!--PRIVACY_NOTE-->", pair(privacy_zh, privacy_en))
                .replace("<!--FOOT_NOTE-->", foot))


def _safe_stem(name: str) -> str:
    """文件名来自客户端请求头，只拿它的主干，且只保留能安全落盘的字符。"""
    stem = Path(name.replace("\\", "/")).stem
    stem = "".join(ch for ch in stem if ch.isprintable() and ch not in '/\\:*?"<>|')
    stem = stem.strip(" .")[:80]
    return stem or "document"


def _image_size(path: Path) -> Optional[tuple[int, int]]:
    """取源 PDF 里满版位图的像素尺寸——算法的坐标系就是它。"""
    with fitz.open(path) as document:
        for page in document:
            images = page.get_images(full=True)
            if images:
                return int(images[0][2]), int(images[0][3])
    return None


def _panel_box(value, size: Optional[tuple[int, int]]) -> Optional[tuple[int, int, int, int]]:
    """把前端传来的比例框换算成位图像素坐标。

    前端传比例而不是像素：预览图是按页面尺寸渲染的，和嵌入位图的像素尺寸并不相等，
    让前端去做这个换算，多一个环节就多一处错。非法输入一律当没传。
    """
    if size is None or not isinstance(value, (list, tuple)) or len(value) != 4:
        return None
    try:
        ratios = [float(v) for v in value]
    except (TypeError, ValueError):
        return None
    if any(r != r or r < -0.1 or r > 1.1 for r in ratios):     # 含 NaN 判断
        return None
    width, height = size
    x0, x1 = sorted((ratios[0] * width, ratios[2] * width))
    y0, y1 = sorted((ratios[1] * height, ratios[3] * height))
    box = (max(0, int(x0)), max(0, int(y0)), min(width, int(x1)), min(height, int(y1)))
    if box[2] - box[0] < 8 or box[3] - box[1] < 4:
        return None
    return box


def _ratio_param(request: Request, key: str, default: float) -> float:
    """检测范围来自查询串。非法值退回默认，合法值也夹在合理区间里。

    夹紧不只是防崩：corner_w=1 意味着拿整页去做跨页交集，CPU 和内存都会翻好几倍。
    """
    raw = request.query_params.get(key)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value != value:  # NaN
        return default
    return min(max(value, 0.05), 0.60)


async def _read_capped_body(request: Request) -> bytes:
    """边收边数，超限立刻断开。

    不能先 await request.body() 再判断大小——那等于把任意大的请求体先收进内存，
    公网上足够拿来打垮进程。Content-Length 只是快速拒绝，不能当真，所以流式那一路
    仍然逐块累计。
    """
    limit_mb = MAX_UPLOAD_BYTES // (1024 * 1024)
    too_big = HTTPException(status_code=413, detail=f"文件超过 {limit_mb} MB")

    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > MAX_UPLOAD_BYTES:
        raise too_big

    chunks: list[bytes] = []
    received = 0
    async for chunk in request.stream():
        received += len(chunk)
        if received > MAX_UPLOAD_BYTES:
            raise too_big
        chunks.append(chunk)
    return b"".join(chunks)


async def _read_small_body(request: Request, limit: int, label: str) -> bytes:
    """小请求也必须边收边计数，避免无 Content-Length 时把大请求全读进内存。"""
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise HTTPException(status_code=413, detail=f"{label}超过大小限制")
    chunks: list[bytes] = []
    received = 0
    async for chunk in request.stream():
        received += len(chunk)
        if received > limit:
            raise HTTPException(status_code=413, detail=f"{label}超过大小限制")
        chunks.append(chunk)
    return b"".join(chunks)


def create_app() -> FastAPI:
    app = FastAPI(title="NotebookLM 去水印", docs_url=None, redoc_url=None)

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse(_render_index())

    # robots.txt 与 sitemap.xml：这个服务没有 StaticFiles 挂载，
    # 每个静态文件都要显式给路由，否则放进 static/ 也取不到。
    @app.get("/robots.txt", response_class=FileResponse)
    def robots_txt() -> FileResponse:
        return FileResponse(STATIC_DIR / "robots.txt", media_type="text/plain",
                            headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/sitemap.xml", response_class=FileResponse)
    def sitemap_xml() -> FileResponse:
        return FileResponse(STATIC_DIR / "sitemap.xml", media_type="application/xml",
                            headers={"Cache-Control": "public, max-age=86400"})

    # 政策页是固定文件而不是任意静态路径，既能被搜索引擎发现，也不会开放目录读取。
    @app.get("/privacy", response_class=FileResponse)
    @app.get("/privacy.html", response_class=FileResponse)
    def privacy_page() -> FileResponse:
        return FileResponse(STATIC_DIR / "privacy.html", media_type="text/html; charset=utf-8",
                            headers={"Cache-Control": "public, max-age=3600"})

    @app.get("/terms", response_class=FileResponse)
    @app.get("/terms.html", response_class=FileResponse)
    def terms_page() -> FileResponse:
        return FileResponse(STATIC_DIR / "terms.html", media_type="text/html; charset=utf-8",
                            headers={"Cache-Control": "public, max-age=3600"})

    @app.get("/about", response_class=FileResponse)
    @app.get("/about.html", response_class=FileResponse)
    def about_page() -> FileResponse:
        return FileResponse(STATIC_DIR / "about.html", media_type="text/html; charset=utf-8",
                            headers={"Cache-Control": "public, max-age=3600"})

    @app.get("/manifest.webmanifest", response_class=FileResponse)
    def web_manifest() -> FileResponse:
        return FileResponse(STATIC_DIR / "manifest.webmanifest",
                            media_type="application/manifest+json")

    @app.get("/service-worker.js", response_class=FileResponse)
    def service_worker() -> FileResponse:
        return FileResponse(STATIC_DIR / "service-worker.js", media_type="text/javascript",
                            headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})

    @app.get("/assets/app-icon.svg", response_class=FileResponse)
    def app_icon() -> FileResponse:
        return FileResponse(STATIC_DIR / "assets" / "app-icon.svg", media_type="image/svg+xml",
                            headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/assets/app-icon-192.png", response_class=FileResponse)
    def app_icon_192() -> FileResponse:
        return FileResponse(STATIC_DIR / "assets" / "app-icon-192.png", media_type="image/png",
                            headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/assets/app-icon-512.png", response_class=FileResponse)
    def app_icon_512() -> FileResponse:
        return FileResponse(STATIC_DIR / "assets" / "app-icon-512.png", media_type="image/png",
                            headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/assets/wechat-qr.png", response_class=FileResponse)
    def wechat_qr() -> FileResponse:
        """公众号二维码是页面自身资源；使用固定路径，不开放任意静态文件读取。"""
        return FileResponse(STATIC_DIR / "assets" / "wechat-qr.png", media_type="image/png")

    @app.get("/assets/support-wechat.png", response_class=FileResponse)
    def support_wechat() -> FileResponse:
        return FileResponse(STATIC_DIR / "assets" / "support-wechat.png", media_type="image/png",
                            headers={"Cache-Control": "no-cache"})

    @app.get("/assets/support-alipay.png", response_class=FileResponse)
    def support_alipay() -> FileResponse:
        return FileResponse(STATIC_DIR / "assets" / "support-alipay.png", media_type="image/png",
                            headers={"Cache-Control": "no-cache"})

    @app.get("/assets/support-binance-usdc-tron.png", response_class=FileResponse)
    def support_binance_usdc_tron() -> FileResponse:
        return FileResponse(STATIC_DIR / "assets" / "support-binance-usdc-tron.png", media_type="image/png",
                            headers={"Cache-Control": "no-cache"})

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        """给部署平台探活用。"""
        with JOBS_LOCK:
            running = sum(1 for job in JOBS.values() if job.status == "running")
        return JSONResponse({"ok": True, "jobs": len(JOBS), "running": running})

    @app.post("/api/jobs")
    async def create_job(request: Request) -> JSONResponse:
        _sweep_expired()
        _check_rate_limit(_client_key(request))
        name = unquote(request.headers.get("x-filename", "") or "").strip() or "document.pdf"
        lowered = name.lower()
        if lowered.endswith(".pptx"):
            kind, suffix = "pptx", ".pptx"
        elif lowered.endswith(".pdf"):
            kind, suffix = "pdf", ".pdf"
        else:
            raise HTTPException(status_code=400, detail="只接受 PDF 或 PPTX 文件")
        payload = await _read_capped_body(request)
        if not payload:
            raise HTTPException(status_code=400, detail="没有收到文件内容")
        # PPTX 是个 zip，头四个字节是 PK\x03\x04；.ppt（老的二进制格式）过不了这一关，
        # 这是对的——那种文件得先另存为 pptx
        magic = b"PK\x03\x04" if kind == "pptx" else b"%PDF"
        if not payload.startswith(magic):
            raise HTTPException(status_code=400,
                                detail=f"这不是一个 {kind.upper()} 文件")

        job_id = uuid.uuid4().hex[:12]
        directory = WORK_ROOT / job_id
        directory.mkdir(parents=True, exist_ok=True)
        stem = _safe_stem(name)
        source = directory / f"{stem}{suffix}"
        source.write_bytes(payload)
        job = Job(job_id, name, source, directory / f"{stem}_已去水印{suffix}", kind=kind)

        if kind == "pptx":
            options = PptxOptions()
        else:
            options = CleanOptions(
                corner_w=_ratio_param(request, "corner_w", 0.30),
                corner_h=_ratio_param(request, "corner_h", 0.12),
            )
        with JOBS_LOCK:
            JOBS[job_id] = job
        _evict_overflow()
        threading.Thread(target=_run_job, args=(job, options), daemon=True).start()
        return JSONResponse({"id": job_id})

    @app.get("/api/jobs/{job_id}")
    def job_status(job_id: str) -> JSONResponse:
        return JSONResponse(_get_job(job_id).snapshot())

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel_job(job_id: str) -> JSONResponse:
        job = _get_job(job_id)
        job.cancelled = True
        return JSONResponse({"ok": True})

    @app.get("/api/jobs/{job_id}/preview")
    def preview(job_id: str, page: int = 1, side: str = "after", zoom: int = 0) -> Response:
        job = _get_job(job_id)
        mode = (job.result or {}).get("mode")
        if job.kind == "pptx" and mode != "bitmaps":
            # 删对象那条路没有「渲染一页」这回事；删了什么直接列出来，比图更准
            raise HTTPException(status_code=409, detail="这份 PPTX 是删对象处理的，请看删除清单")
        path = job.source if side == "before" else (job.decorated if side == "decorated" else job.destination)
        if not path.exists():
            raise HTTPException(status_code=404, detail="该版本尚未生成")
        try:
            image = (render_slide(path, page - 1) if job.kind == "pptx"
                     else render_page(path, page - 1))
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        box_ratio = (job.result or {}).get("box_ratio") if zoom else None
        return _png(image, box_ratio)

    def _decoration_options(request: Request) -> tuple[str, float, float, float | None, float | None]:
        position = request.query_params.get("position", "bottom-right")
        if position not in POSITIONS:
            raise HTTPException(status_code=400, detail="位置参数无效")
        try:
            opacity = float(request.query_params.get("opacity", ".8"))
            margin = float(request.query_params.get("margin", ".03"))
            x_raw, y_raw = request.query_params.get("x"), request.query_params.get("y")
            x = float(x_raw) if x_raw is not None else None
            y = float(y_raw) if y_raw is not None else None
        except ValueError:
            raise HTTPException(status_code=400, detail="透明度或边距参数无效")
        if not .1 <= opacity <= 1 or not 0 <= margin <= .15:
            raise HTTPException(status_code=400, detail="透明度或边距超出范围")
        if (x is None) != (y is None) or (x is not None and not (0 <= x <= 1 and 0 <= y <= 1)):
            raise HTTPException(status_code=400, detail="拖动位置参数无效")
        return position, opacity, margin, x, y

    def _hex_color(value: str, *, optional: bool = False) -> tuple[float, float, float] | None:
        if optional and value in {"", "none", "transparent"}:
            return None
        value = value.lstrip("#")
        if len(value) != 6 or any(char not in "0123456789abcdefABCDEF" for char in value):
            raise HTTPException(status_code=400, detail="颜色参数无效")
        return tuple(int(value[index:index + 2], 16) / 255 for index in (0, 2, 4))

    def _ready_pdf(job_id: str) -> Job:
        job = _get_job(job_id)
        if job.kind != "pdf":
            raise HTTPException(status_code=400, detail="导出增强功能第一版仅支持 PDF")
        if job.status != "done" or not job.destination.exists():
            raise HTTPException(status_code=409, detail="请等待去水印完成")
        return job

    @app.post("/api/jobs/{job_id}/decorate/logo")
    async def decorate_logo(job_id: str, request: Request) -> JSONResponse:
        job = _ready_pdf(job_id)
        position, opacity, margin, x, y = _decoration_options(request)
        try:
            width = float(request.query_params.get("width", ".16"))
        except ValueError:
            raise HTTPException(status_code=400, detail="Logo 大小参数无效")
        if not .05 <= width <= .4:
            raise HTTPException(status_code=400, detail="Logo 宽度需为页面的 5%–40%")
        payload = await _read_small_body(request, 5 * 1024 * 1024, "Logo 图片")
        if not payload:
            raise HTTPException(status_code=400, detail="没有收到 Logo 图片")
        output = job.directory / f"{job.destination.stem}_加Logo.pdf"
        try:
            add_logo(job.destination, output, payload, position=position,
                     width_ratio=width, opacity=opacity, margin_ratio=margin,
                     x_ratio=x, y_ratio=y)
        except (ValueError, OSError) as exc:
            raise HTTPException(status_code=400, detail=f"Logo 图片无法使用：{exc}")
        job.decorated = output
        return JSONResponse({"ok": True, "download": f"/api/jobs/{job.id}/download?variant=decorated"})

    @app.post("/api/jobs/{job_id}/decorate/text")
    async def decorate_text(job_id: str, request: Request) -> JSONResponse:
        job = _ready_pdf(job_id)
        position, opacity, margin, x, y = _decoration_options(request)
        try:
            size = float(request.query_params.get("size", ".035"))
        except ValueError:
            raise HTTPException(status_code=400, detail="文字大小参数无效")
        if not .015 <= size <= .1:
            raise HTTPException(status_code=400, detail="文字大小超出范围")
        font = request.query_params.get("font", "cjk")
        if font not in FONTS:
            raise HTTPException(status_code=400, detail="字体参数无效")
        color = _hex_color(request.query_params.get("color", "#1f2937"))
        background = _hex_color(request.query_params.get("background", "none"), optional=True)
        try:
            raw = await _read_small_body(request, 8 * 1024, "文字内容")
            body = json.loads(raw or b"{}")
            text = body.get("text", "")
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise HTTPException(status_code=400, detail="文字内容无效")
        if not isinstance(text, str):
            raise HTTPException(status_code=400, detail="文字内容无效")
        output = job.directory / f"{job.destination.stem}_加文字.pdf"
        try:
            add_text(job.destination, output, text, position=position,
                     size_ratio=size, opacity=opacity, margin_ratio=margin,
                     x_ratio=x, y_ratio=y, font=font, color=color,
                     background=background)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        job.decorated = output
        return JSONResponse({"ok": True, "download": f"/api/jobs/{job.id}/download?variant=decorated"})

    @app.post("/api/jobs/{job_id}/marks")
    async def rerun_with_marks(job_id: str, request: Request) -> JSONResponse:
        """按用户点名的对象重跑 PPTX。

        自动模式只动落在正文区之外的小东西；压在正文里的重复块会被列出来但不碰，
        要删得由人点名——删对象是不可见的操作，不该替用户做主。
        """
        job = _get_job(job_id)
        if job.kind != "pptx":
            raise HTTPException(status_code=400, detail="这个接口只用于 PPTX")
        if job.status == "running":
            raise HTTPException(status_code=409, detail="任务还在跑")
        try:
            body = json.loads(await _read_capped_body(request) or b"{}")
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="请求体不是合法 JSON")
        keys = body.get("keys")
        if not isinstance(keys, list) or not all(isinstance(k, str) for k in keys):
            raise HTTPException(status_code=400, detail="keys 必须是字符串数组")
        if not job.source.exists():
            raise HTTPException(status_code=410, detail="源文件已过期")

        job.status, job.stage, job.error, job.result = "running", "重新清理…", "", None
        threading.Thread(target=_run_job, args=(job, PptxOptions(), keys or None),
                         daemon=True).start()
        return JSONResponse({"id": job.id})

    @app.post("/api/jobs/{job_id}/reprocess")
    async def reprocess(job_id: str, request: Request) -> JSONResponse:
        """带着用户框定的衬底范围，把同一份源文件再跑一遍。

        不要求重新上传：源文件还在任务目录里。用户看到残留 → 框出来 → 再跑，
        这个来回要足够快才有人愿意用。
        """
        job = _get_job(job_id)
        if job.status == "running":
            raise HTTPException(status_code=409, detail="上一次处理还没结束")
        if not job.source.exists():
            raise HTTPException(status_code=410, detail="源文件已过期，请重新上传")

        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="请求体不是合法 JSON")
        panel = _panel_box(payload.get("panel_ratio"), _image_size(job.source))
        if payload.get("panel_ratio") is not None and panel is None:
            raise HTTPException(status_code=400, detail="框选范围无效，请重新框一次")
        options = CleanOptions(
            corner_w=_ratio_param(request, "corner_w", 0.30),
            corner_h=_ratio_param(request, "corner_h", 0.12),
            panel_box=panel,
        )
        job.status, job.stage, job.error = "running", "重新处理中…", ""
        job.current, job.total, job.result = 0, 0, None
        job.cancelled = False
        threading.Thread(target=_run_job, args=(job, options), daemon=True).start()
        return JSONResponse({"ok": True, "panel_box": panel})

    @app.get("/api/jobs/{job_id}/download")
    def download(job_id: str, variant: str = "clean") -> FileResponse:
        job = _get_job(job_id)
        if job.status != "done" or not job.destination.exists():
            raise HTTPException(status_code=409, detail="任务尚未成功完成")
        target = job.decorated if variant == "decorated" else job.destination
        if target is None or not target.exists():
            raise HTTPException(status_code=404, detail="增强导出文件尚未生成")
        media = ("application/vnd.openxmlformats-officedocument.presentationml.presentation"
                 if job.kind == "pptx" else "application/pdf")
        # no-transform 是给中间层看的：别动这个响应体。
        # 实测走 Cloudflare 时，只要浏览器发了 Accept-Encoding: gzip/br，
        # 同一个 18.8 MB 文件的下载会从 2 秒掉到 120～200 秒（8 MB/s → 90 KB/s），
        # 而响应里根本没有 content-encoding——白挨一遍压缩，还把传输拖垮。
        # PDF 和 PPTX 本来就是压缩过的容器，再压一遍毫无收益。
        return FileResponse(target, media_type=media,
                            filename=target.name,
                            headers={"Cache-Control": "no-transform"})

    @app.get("/api/batch/download")
    def download_batch(jobs: str = "") -> FileResponse:
        """把一批已完成结果打成无压缩 ZIP；容器文件本身已压缩，重复压缩只会拖慢下载。"""
        ids = list(dict.fromkeys(part.strip() for part in jobs.split(",") if part.strip()))
        if not ids or len(ids) > 10:
            raise HTTPException(status_code=400, detail="批量下载需要 1 到 10 个任务")
        selected = [_get_job(job_id) for job_id in ids]
        if any(job.status != "done" or not job.destination.exists() for job in selected):
            raise HTTPException(status_code=409, detail="批量任务中有文件尚未成功完成")

        fd, archive_name = tempfile.mkstemp(prefix="unmark_batch_", suffix=".zip")
        os.close(fd)
        used: set[str] = set()
        with zipfile.ZipFile(archive_name, "w", compression=zipfile.ZIP_STORED) as archive:
            for index, job in enumerate(selected, 1):
                name = job.destination.name
                if name in used:
                    name = f"{Path(name).stem}_{index}{Path(name).suffix}"
                used.add(name)
                archive.write(job.destination, arcname=name)
        return FileResponse(
            archive_name,
            media_type="application/zip",
            filename="unmark_批量处理结果.zip",
            headers={"Cache-Control": "no-transform"},
            background=BackgroundTask(os.unlink, archive_name),
        )

    @app.delete("/api/jobs/{job_id}")
    def drop(job_id: str) -> JSONResponse:
        job = _get_job(job_id)
        with JOBS_LOCK:
            JOBS.pop(job_id, None)
        shutil.rmtree(job.directory, ignore_errors=True)
        return JSONResponse({"ok": True})

    return app


app = create_app()


def main() -> None:
    import argparse
    import webbrowser

    import uvicorn

    parser = argparse.ArgumentParser(description="NotebookLM 去水印网页版")
    parser.add_argument("--port", type=int, default=_env_int("PORT", 8823))
    parser.add_argument("--host", default=os.environ.get("UNMARK_HOST", "127.0.0.1"),
                        help="监听地址。默认只有本机能访问；公网部署用 0.0.0.0")
    parser.add_argument("--no-browser", action="store_true", help="不自动打开浏览器")
    args = parser.parse_args()

    local_only = args.host in ("127.0.0.1", "localhost", "::1")
    url = f"http://127.0.0.1:{args.port}/"
    if local_only:
        print(f"NotebookLM 去水印  →  {url}\n按 Control-C 退出。PDF 全程只在本机处理。")
        if not args.no_browser:
            threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    else:
        print(
            f"NotebookLM 去水印  →  对外监听 {args.host}:{args.port}\n"
            f"上限 {MAX_UPLOAD_BYTES // (1024 * 1024)} MB／并发 {MAX_CONCURRENT_JOBS}／"
            f"限流 {RATE_LIMIT_JOBS} 次每 {int(RATE_LIMIT_WINDOW.total_seconds() // 60)} 分钟／"
            f"任务保留 {int(JOB_TTL.total_seconds() // 60)} 分钟"
        )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
