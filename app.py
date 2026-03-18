#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
教材知识点梳理与旧库去重工具 v2.0
==================================
使用方式：python3 app.py
然后浏览器打开 http://localhost:8686
"""

import os
import sys
import json
import re
import html as html_mod
import time
import uuid
import threading
import traceback
import urllib.request
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path
from difflib import SequenceMatcher

# ============================================================
# 配置
# ============================================================
PORT = 8686
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================
# 百度OCR配置（扫描版PDF专用）
# ============================================================
BAIDU_APP_ID     = "122411799"
BAIDU_API_KEY    = "tShoflxPjqFfUdQI0xjuTQuo"
BAIDU_SECRET_KEY = "kCWeOSxvxueIIShFwWDUy5xLzR3KlgEA"  # ⚠️ 请替换为重置后的新Key
POPPLER_PATH     = r"D:\poppler\Library\bin"

# ============================================================
# PDF文本提取（自动识别电子版/扫描版）
# ============================================================
def extract_pdf_text(pdf_path):
    """从PDF中提取文本，自动判断电子版/扫描版，扫描版走百度OCR"""
    import pdfplumber

    # ── 第一步：先尝试直接提取文字（电子版PDF）──
    pages = {}
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            pages[i + 1] = text

    total_chars = sum(len(t) for t in pages.values())

    # ── 提取到足够文字，直接返回（电子版）──
    if total_chars > 100:
        return pages

    # ── 文字太少，说明是扫描版，启动百度OCR ──
    print("⚠️  检测到扫描版PDF，启动百度OCR识别...")

    try:
        from aip import AipOcr
        from pdf2image import convert_from_path
        import base64, io

        client = AipOcr(BAIDU_APP_ID, BAIDU_API_KEY, BAIDU_SECRET_KEY)

        with pdfplumber.open(pdf_path) as pdf:
            total = len(pdf.pages)

        ocr_pages = {}
        BATCH = 10  # 每批10页，防止内存溢出

        for batch_start in range(0, total, BATCH):
            batch_end = min(batch_start + BATCH, total)
            print(f"  OCR处理第 {batch_start+1}~{batch_end} 页 / 共{total}页...")

            images = convert_from_path(
                pdf_path,
                first_page=batch_start + 1,
                last_page=batch_end,
                dpi=150,              # 150dpi够用，更高会超百度图片大小限制4MB
                poppler_path=POPPLER_PATH
            )

            for j, img in enumerate(images):
                page_num = batch_start + j + 1

                # 图片转base64发给百度
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                img_b64 = base64.b64encode(buf.getvalue()).decode()

                # 调用百度高精度OCR
                result = client.basicAccurate(base64.b64decode(img_b64))

                if "words_result" in result:
                    text = "\n".join(
                        w["words"] for w in result["words_result"]
                    )
                else:
                    text = ""
                    print(f"  ⚠️  第{page_num}页识别失败：{result.get('error_msg', '未知错误')}")

                ocr_pages[page_num] = text

                # 百度免费版限速：每秒2次，加延迟避免超限报错
                time.sleep(0.6)

        print(f"✅ OCR完成，共识别 {total} 页")
        return ocr_pages

    except ImportError as e:
        print(f"❌ 缺少依赖库：{e}")
        print("请运行：pip install baidu-aip pdf2image Pillow")
        return pages


# ============================================================
# 目录解析与章节分割（改进版）
# ============================================================
def parse_toc_entries(pages_dict):
    """从PDF页面中解析目录，返回结构化的目录条目列表"""
    toc_pages_text = ""
    for pnum in sorted(pages_dict.keys()):
        text = pages_dict[pnum]
        if "目录" in text or "目 录" in text:
            toc_pages_text += text + "\n"
            for next_p in range(pnum + 1, pnum + 4):
                if next_p in pages_dict:
                    next_text = pages_dict[next_p]
                    if re.search(r"…+\s*\d+", next_text):
                        toc_pages_text += next_text + "\n"
                    else:
                        break
            break

    if not toc_pages_text:
        return []

    entries = []
    current_chapter = ""

    for line in toc_pages_text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # 匹配简单数字章节（一、二、三...）
        simple_ch_match = re.search(r"^([一二三四五六七八九十]+)[、\s]+(.+?)…+\s*(\d+)", line)
        if simple_ch_match:
            current_chapter = f"{simple_ch_match.group(1)} {simple_ch_match.group(2).strip()}"
            entries.append({
                "type": "chapter",
                "chapter": current_chapter,
                "section": "",
                "title": simple_ch_match.group(2).strip(),
                "page": int(simple_ch_match.group(3)),
            })
            continue

        # 匹配无数字前缀的章节（如"年、月、日的奥秘"）
        # 必须在行首，不以数字或"第"开头，包含省略号和页码
        no_prefix_match = re.search(r"^([^一二三四五六七八九十第\d\s].{2,30}?)…+\s*(\d+)$", line)
        if no_prefix_match:
            title = no_prefix_match.group(1).strip()
            # 排除小节标题（通常以数字开头如"1."或"2."）
            if not re.match(r"^\d+[\.、]", title) and len(entries) < 15:  # 限制章节数量避免误匹配
                current_chapter = title
                entries.append({
                    "type": "chapter",
                    "chapter": current_chapter,
                    "section": "",
                    "title": title,
                    "page": int(no_prefix_match.group(2)),
                })
                continue

        # 匹配章标题
        ch_match = re.search(r"(第[一二三四五六七八九十百]+章)\s+(.+?)…+\s*(\d+)", line)
        if ch_match:
            current_chapter = f"{ch_match.group(1)} {ch_match.group(2).strip()}"
            entries.append({
                "type": "chapter",
                "chapter": current_chapter,
                "section": "",
                "title": ch_match.group(2).strip(),
                "page": int(ch_match.group(3)),
            })
            continue

        # 匹配单元标题（历史等）
        unit_match = re.search(r"(第[一二三四五六七八九十百]+单元)\s+(.+?)…+\s*(\d+)", line)
        if unit_match:
            current_chapter = f"{unit_match.group(1)} {unit_match.group(2).strip()}"
            entries.append({
                "type": "chapter",
                "chapter": current_chapter,
                "section": "",
                "title": unit_match.group(2).strip(),
                "page": int(unit_match.group(3)),
            })
            continue

        # 匹配节标题
        sec_match = re.search(r"(第[一二三四五六七八九十百]+节)\s*(.+?)…+\s*(\d+)", line)
        if sec_match:
            section_name = f"{sec_match.group(1)} {sec_match.group(2).strip()}"
            entries.append({
                "type": "section",
                "chapter": current_chapter,
                "section": section_name,
                "title": sec_match.group(2).strip(),
                "page": int(sec_match.group(3)),
            })
            continue

        # 匹配课标题（历史等）
        lesson_match = re.search(r"(第\s*\d+\s*课)\s*(.+?)…+\s*(\d+)", line)
        if lesson_match:
            lesson_title = lesson_match.group(2).strip()
            # 活动课标记为跳过
            if "活动课" in lesson_match.group(0) or "活动课" in lesson_title:
                entries.append({
                    "type": "skip",
                    "chapter": current_chapter,
                    "section": f"{lesson_match.group(1).replace(' ','')} {lesson_title}",
                    "title": lesson_title,
                    "page": int(lesson_match.group(3)),
                })
                continue
            section_name = f"{lesson_match.group(1).replace(' ','')} {lesson_title}"
            entries.append({
                "type": "section",
                "chapter": current_chapter,
                "section": section_name,
                "title": lesson_title,
                "page": int(lesson_match.group(3)),
            })
            continue

        # 匹配跳过项（跨学科、附录、学史方法等）
        skip_match = re.search(r"(【.+?】|跨学科|附录|本书常用|学史方法|活动课|大事年表).+?…+\s*(\d+)", line)
        if skip_match:
            entries.append({
                "type": "skip",
                "chapter": "",
                "section": skip_match.group(0).split("…")[0].strip(),
                "title": skip_match.group(0).split("…")[0].strip(),
                "page": int(skip_match.group(2)),
            })

    # 添加顺序索引
    for i, entry in enumerate(entries):
        entry["order_index"] = i

    return entries


def split_pages_by_toc(pages_dict, toc_entries):
    """根据目录条目把页面文本分配到各章节"""
    if not toc_entries:
        return split_pages_by_regex(pages_dict)

    # 校准PDF页码偏移
    offset = 0
    for entry in toc_entries:
        if entry["type"] in ("chapter", "section"):
            target_title = entry["title"][:6]
            target_page = entry["page"]
            for pdf_page in sorted(pages_dict.keys()):
                if target_title in pages_dict[pdf_page]:
                    offset = pdf_page - target_page
                    break
            break

    for entry in toc_entries:
        entry["pdf_page"] = entry["page"] + offset

    valid_entries = [e for e in toc_entries if e["type"] in ("chapter", "section")]
    skip_pages = set()
    for e in toc_entries:
        if e["type"] == "skip":
            # 标记skip条目对应的页码范围
            skip_pages.add(e["pdf_page"])

    chunks = []
    for i, entry in enumerate(valid_entries):
        start_page = entry["pdf_page"]
        if i + 1 < len(valid_entries):
            end_page = valid_entries[i + 1]["pdf_page"] - 1
        else:
            end_page = max(pages_dict.keys())

        # 检查是否跨进了skip区域
        all_toc = sorted(toc_entries, key=lambda e: e["pdf_page"])
        for te in all_toc:
            if te["type"] == "skip" and start_page < te["pdf_page"] <= end_page:
                end_page = te["pdf_page"] - 1
                break

        text_parts = []
        for pnum in range(start_page, end_page + 1):
            if pnum in pages_dict and pnum not in skip_pages:
                text_parts.append(pages_dict[pnum])

        combined_text = "\n".join(text_parts)
        if len(combined_text.strip()) < 30:
            continue

        chunks.append({
            "chapter": entry["chapter"],
            "section": entry["section"] if entry["type"] == "section" else "",
            "chapter_order": entry.get("order_index", 0),
            "info": f"{entry['chapter']} {entry['section']}".strip(),
            "text": combined_text,
        })

    return chunks


def split_pages_by_regex(pages_dict):
    """后备方案：用正则从正文中检测章节边界"""
    chunks = []
    current_chapter = ""
    current_section = ""
    current_text = ""

    skip_keywords = ["跨学科主题学习", "附录", "本书常用地图图例", "活动课", "学史方法", "大事年表"]

    for pnum in sorted(pages_dict.keys()):
        if pnum <= 3:
            continue
        text = pages_dict[pnum]

        should_skip = any(kw in text[:150] for kw in skip_keywords)
        if should_skip:
            if current_text.strip():
                chunks.append({"chapter": current_chapter, "section": current_section,
                               "info": f"{current_chapter} {current_section}".strip(),
                               "text": current_text})
                current_text = ""
            continue

        ch = re.search(r"(第[一二三四五六七八九十百]+章)\s+(.+?)(?:\n|$)", text)
        sec = re.search(r"(第[一二三四五六七八九十百]+节)\s*(.+?)(?:\n|$)", text)
        lesson = re.search(r"(第\s*\d+\s*课)\s*(.+?)(?:\n|$)", text)
        unit = re.search(r"(第[一二三四五六七八九十百]+单元)\s+(.+?)(?:\n|$)", text)

        if sec or lesson:
            if current_text.strip():
                chunks.append({"chapter": current_chapter, "section": current_section,
                               "info": f"{current_chapter} {current_section}".strip(),
                               "text": current_text})
            if ch:
                current_chapter = f"{ch.group(1)} {ch.group(2).strip()}"
            elif unit:
                current_chapter = f"{unit.group(1)} {unit.group(2).strip()}"
            m = sec or lesson
            current_section = f"{m.group(1).replace(' ','')} {m.group(2).strip()}"
            current_text = text
        elif ch or unit:
            if current_text.strip():
                chunks.append({"chapter": current_chapter, "section": current_section,
                               "info": f"{current_chapter} {current_section}".strip(),
                               "text": current_text})
            m = ch or unit
            current_chapter = f"{m.group(1)} {m.group(2).strip()}"
            current_section = ""
            current_text = text
        else:
            current_text += "\n" + text

    if current_text.strip():
        chunks.append({"chapter": current_chapter, "section": current_section,
                       "info": f"{current_chapter} {current_section}".strip(),
                       "text": current_text})

    return [c for c in chunks if len(c["text"].strip()) > 80]


# ============================================================
# Excel解析
# ============================================================
def parse_old_kb(xlsx_path):
    """解析旧库Excel"""
    import openpyxl
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    headers = []
    for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
        headers = [str(c).strip() if c else "" for c in row]
        break

    col_map = {h: i for i, h in enumerate(headers)}

    records = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        vals = list(row)
        if not vals or all(v is None for v in vals):
            continue

        def get(name, default=""):
            idx = col_map.get(name)
            if idx is not None and idx < len(vals) and vals[idx] is not None:
                return str(vals[idx]).strip()
            return default

        subject_raw = get("subjectInner") or get("学科") or get("subject") or ""
        kid = get("知识点Kid") or get("一级知识点id") or get("Kid") or ""
        kname = get("知识点名称") or get("一级知识点") or ""
        sub_title = get("二级知识点标题") or ""
        sub_detail = get("二级知识点详情") or ""
        chapter = get("篇章名称") or get("章") or ""
        section = get("模块名称") or get("节") or ""

        if not sub_title and not kname:
            continue

        clean_detail = strip_html(sub_detail)
        records.append({
            "subject": normalize_subject(subject_raw),
            "kid": kid,
            "knowledge_name": kname,
            "sub_title": sub_title,
            "sub_detail": clean_detail,
            "sub_detail_short": clean_detail[:150],
            "chapter": chapter,
            "section": section,
            "period": get("学段") or "",
            "grade": get("年级") or "",
            "publisher": get("出版社") or "",
            "volume": get("volume") or get("册") or "",
        })

    wb.close()
    return records


def parse_ref_table(xlsx_path):
    """解析参考表格"""
    import openpyxl
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    headers = []
    for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
        headers = [str(c).strip() if c else "" for c in row]
        break

    col_map = {h: i for i, h in enumerate(headers)}
    samples = []

    for row in ws.iter_rows(min_row=2, values_only=True):
        if len(samples) >= 30:
            break
        vals = list(row)
        if not vals or all(v is None for v in vals):
            continue

        def get(name, default=""):
            idx = col_map.get(name)
            if idx is not None and idx < len(vals) and vals[idx] is not None:
                return str(vals[idx]).strip()
            return default

        samples.append({
            "章": get("章") or get("篇章名称"),
            "节": get("节") or get("模块名称"),
            "一级知识点": get("一级知识点") or get("知识点名称"),
            "一级知识点id": get("一级知识点id") or get("知识点Kid"),
            "二级知识点标题": get("二级知识点标题"),
            "二级知识点详情": strip_html(get("二级知识点详情"))[:200],
            "备注": get("备注"),
        })

    wb.close()
    return samples


def detect_subjects_in_kb(xlsx_path):
    """快速扫描旧库中有哪些学科"""
    import openpyxl
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    headers = []
    for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
        headers = [str(c).strip() if c else "" for c in row]
        break

    col_map = {h: i for i, h in enumerate(headers)}
    subj_idx = col_map.get("subjectInner", col_map.get("学科", col_map.get("subject")))

    subjects = {}
    if subj_idx is None:
        wb.close()
        return []

    for row in ws.iter_rows(min_row=2, values_only=True):
        vals = list(row)
        if subj_idx < len(vals) and vals[subj_idx]:
            raw = str(vals[subj_idx]).strip()
            norm = normalize_subject(raw)
            subjects[norm] = subjects.get(norm, 0) + 1

    wb.close()
    return sorted(subjects.items(), key=lambda x: -x[1])


def detect_kb_filters(xlsx_path):
    """扫描旧库中所有可筛选维度：学科、学段、年级、出版社、册"""
    import openpyxl
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    headers = []
    for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
        headers = [str(c).strip() if c else "" for c in row]
        break

    col_map = {h: i for i, h in enumerate(headers)}
    dims = {"subjects": {}, "periods": {}, "grades": {}, "publishers": {}, "volumes": {}}
    field_map = {
        "subjects": col_map.get("subjectInner", col_map.get("学科")),
        "periods": col_map.get("学段"),
        "grades": col_map.get("年级"),
        "publishers": col_map.get("出版社"),
        "volumes": col_map.get("volume", col_map.get("册")),
    }

    for row in ws.iter_rows(min_row=2, values_only=True):
        vals = list(row)
        for dim, idx in field_map.items():
            if idx is not None and idx < len(vals) and vals[idx]:
                val = str(vals[idx]).strip()
                if dim == "subjects":
                    val = normalize_subject(val)
                if val:
                    dims[dim][val] = dims[dim].get(val, 0) + 1

    wb.close()
    return {k: sorted(v.items(), key=lambda x: -x[1]) for k, v in dims.items()}


# ============================================================
# 工具函数
# ============================================================
SUBJECT_MAP = {
    "历史": "历史", "中国历史": "历史", "世界历史": "历史",
    "生物": "生物", "生物学": "生物",
    "政治": "政治", "道德与法治": "政治",
    "地理": "地理", "化学": "化学", "物理": "物理", "数学": "数学",
}

def normalize_subject(s):
    if not s:
        return s
    s = s.strip()
    for key, val in SUBJECT_MAP.items():
        if key in s:
            return val
    return s

def strip_html(text):
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p>", "\n", text, flags=re.I)
    text = re.sub(r"</div>", "\n", text, flags=re.I)
    text = re.sub(r"</li>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_mod.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def text_similarity(a, b):
    if not a or not b:
        return 0.0
    la, lb = len(a), len(b)
    if la == 0 or lb == 0:
        return 0.0
    if max(la, lb) / max(min(la, lb), 1) > 5:
        return 0.1
    return SequenceMatcher(None, a, b).ratio()

def keyword_overlap(a, b):
    if not a or not b:
        return 0.0
    def tokenize(s):
        tokens = re.split(r'[，。、；：\s,.:;!?()（）\[\]【】""''\u00b7\u2014-]+', s)
        return set(t for t in tokens if len(t) >= 2)
    sa, sb = tokenize(a), tokenize(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(min(len(sa), len(sb)), 1)

def sanitize_excel_value(val):
    """移除Excel不支持的非法字符（控制字符等）"""
    if not isinstance(val, str):
        return val
    # 移除控制字符（0x00-0x1F，除了\t\n\r）和其他非法字符
    return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', val)


# ============================================================
# 旧库匹配
# ============================================================
def prefilter_kb_for_chapter(subject_kb, chapter_name, section_name, new_title="", new_k1=""):
    """根据章节名称和知识点标题预筛选相关旧库记录"""
    keywords = set()
    for name in [chapter_name, section_name, new_title, new_k1]:
        tokens = re.split(r'[第一二三四五六七八九十百章节课单元\s——""（）\-的与和及其在从到]+', name)
        keywords.update(t for t in tokens if len(t) >= 2)

    if not keywords and not new_title:
        return subject_kb[:800]

    scored = []
    for r in subject_kb:
        score = 0
        text = f"{r['chapter']} {r['section']} {r['knowledge_name']} {r['sub_title']}"
        for kw in keywords:
            if kw in text:
                score += 1

        # Title containment bonus: if new_title contains KB sub_title or vice versa
        if new_title and r["sub_title"] and len(r["sub_title"]) >= 3:
            if r["sub_title"] in new_title or new_title in r["sub_title"]:
                score += 5  # Strong signal

        scored.append((score, r))

    scored.sort(key=lambda x: -x[0])
    # Take all with score > 0, up to 800
    result = [r for s, r in scored if s > 0][:800]
    # If too few hits, pad with top records
    if len(result) < 200:
        seen = set(id(r) for r in result)
        for _, r in scored:
            if id(r) not in seen:
                result.append(r)
                if len(result) >= 400:
                    break
    return result


def match_with_old_kb(new_points, old_kb_records, api_config=None, log_fn=None, match_config=None):
    if log_fn is None:
        log_fn = lambda msg, t="info": None
    if match_config is None:
        match_config = {}

    threshold = match_config.get("threshold", 70) / 100.0
    w_title = match_config.get("w_title", 50) / 100.0
    w_detail = match_config.get("w_detail", 40) / 100.0
    w_k1 = match_config.get("w_k1", 10) / 100.0
    use_ai = match_config.get("use_ai", True)

    log_fn(f"🔍 代码匹配 ({len(new_points)} 条 vs {len(old_kb_records)} 条旧库)")
    log_fn(f"  参数: 阈值={threshold:.0%} 二级标题={w_title:.0%} 二级详情={w_detail:.0%} 一级知识点={w_k1:.0%}")

    matched_results = []
    unmatched = []

    for point in new_points:
        best_match = None
        best_score = 0

        new_title = point.get("二级知识点标题", "")
        new_detail = point.get("二级知识点详情", "")
        new_k1 = point.get("一级知识点", "")

        candidates = prefilter_kb_for_chapter(
            old_kb_records, point.get("章", ""), point.get("节", ""),
            new_title, new_k1
        )

        for kb in candidates:
            title_sim = text_similarity(new_title, kb["sub_title"])

            # Fast path 1: title nearly identical → high confidence
            if title_sim >= 0.85:
                score = 0.85 + title_sim * 0.15
            # Fast path 2: one title contains the other (e.g. "土地改革" in "土地改革的意义")
            elif (len(new_title) >= 3 and len(kb["sub_title"]) >= 3 and
                  (new_title in kb["sub_title"] or kb["sub_title"] in new_title)):
                score = 0.80 + title_sim * 0.10
            else:
                detail_kw = keyword_overlap(new_detail[:80], kb["sub_detail_short"])
                detail_sim = text_similarity(new_detail[:80], kb["sub_detail_short"])
                detail_score = max(detail_kw, detail_sim)

                k1_sim = text_similarity(new_k1, kb["knowledge_name"])
                score = title_sim * w_title + detail_score * w_detail + k1_sim * w_k1

            if score > best_score:
                best_score = score
                best_match = kb

        if best_score >= threshold and best_match:
            matched_results.append({
                "章": point.get("章", ""),
                "节": point.get("节", ""),
                "chapter_order": point.get("chapter_order", 0),
                "一级知识点": best_match["knowledge_name"],
                "一级知识点id": best_match["kid"],
                "二级知识点标题": best_match["sub_title"],
                "二级知识点详情": best_match["sub_detail"],
                "备注": "已存在，无需新增",
                "_confidence": round(best_score * 100),
                "_new_title": point.get("二级知识点标题", ""),
                "_new_detail": point.get("二级知识点详情", ""),
                "_kb_chapter": best_match.get("chapter", ""),
                "_kb_section": best_match.get("section", ""),
            })
        else:
            point["_best_score"] = round(best_score, 3)
            unmatched.append(point)

    log_fn(f"✅ 代码匹配完成：匹配 {len(matched_results)} 条，未匹配 {len(unmatched)} 条", "success")

    if unmatched and use_ai and api_config and api_config.get("api_key"):
        log_fn(f"🤖 AI语义比对 {len(unmatched)} 条")
        ai_results = ai_semantic_match(unmatched, old_kb_records, api_config, log_fn)
        matched_results.extend(ai_results)
    else:
        for point in unmatched:
            matched_results.append({
                "章": point.get("章", ""),
                "节": point.get("节", ""),
                "chapter_order": point.get("chapter_order", 0),
                "一级知识点": point.get("一级知识点", ""),
                "一级知识点id": "",
                "二级知识点标题": point.get("二级知识点标题", ""),
                "二级知识点详情": point.get("二级知识点详情", ""),
                "备注": "需新增",
                "_confidence": 0,
                "_new_title": "",
                "_new_detail": "",
                "_kb_chapter": "",
                "_kb_section": "",
            })
        if unmatched:
            log_fn(f"⏭️ 跳过AI比对，{len(unmatched)} 条标记为需新增")

    return matched_results


def ai_semantic_match(unmatched_points, old_kb_records, api_config, log_fn):
    results = []
    batch_size = 8

    for i in range(0, len(unmatched_points), batch_size):
        batch = unmatched_points[i:i + batch_size]

        # 为batch预筛选旧库
        relevant_ids = set()
        for pt in batch:
            cands = prefilter_kb_for_chapter(
                old_kb_records, pt.get("章", ""), pt.get("节", ""),
                pt.get("二级知识点标题", ""), pt.get("一级知识点", "")
            )
            for c in cands[:80]:
                relevant_ids.add(id(c))

        kb_list = [r for r in old_kb_records if id(r) in relevant_ids][:150]
        kb_text = "\n".join([
            f"Kid:{r['kid']}|名称:{r['knowledge_name']}|标题:{r['sub_title']}|详情:{r['sub_detail_short']}"
            for r in kb_list
        ])

        batch_json = json.dumps([
            {"index": j, "一级知识点": p.get("一级知识点",""),
             "二级知识点标题": p.get("二级知识点标题",""),
             "二级知识点详情": p.get("二级知识点详情","")[:200]}
            for j, p in enumerate(batch)
        ], ensure_ascii=False)

        prompt = f"""判断以下新教材知识点是否与旧库中已有知识点语义相近。

新教材知识点：
{batch_json}

旧库知识点：
{kb_text}

规则：
1. 比较语义，看考点角度是否一致。允许不同表述但含义相同的匹配
2. 新教材有新考点角度（旧库未涵盖），判为不匹配
3. 匹配到多个只选最贴近的一个

输出JSON数组：
{{"index":0,"matched":true/false,"kid":"匹配的Kid或空","knowledge_name":"","sub_title":"","sub_detail":""}}

只输出JSON数组。"""

        try:
            ai_result = call_llm_api(api_config, prompt)
            parsed = parse_json_response(ai_result)
            parsed_map = {item.get("index", -1): item for item in parsed}

            for j, point in enumerate(batch):
                item = parsed_map.get(j)
                if item and item.get("matched"):
                    results.append({
                        "章": point.get("章", ""),
                        "节": point.get("节", ""),
                        "chapter_order": point.get("chapter_order", 0),
                        "一级知识点": item.get("knowledge_name") or point.get("一级知识点", ""),
                        "一级知识点id": item.get("kid", ""),
                        "二级知识点标题": item.get("sub_title") or point.get("二级知识点标题", ""),
                        "二级知识点详情": strip_html(item.get("sub_detail","")) or point.get("二级知识点详情",""),
                        "备注": "已存在，无需新增",
                        "_confidence": -1,
                        "_new_title": point.get("二级知识点标题", ""),
                        "_new_detail": point.get("二级知识点详情", ""),
                        "_kb_chapter": "",
                        "_kb_section": "",
                    })
                else:
                    results.append({
                        "章": point.get("章", ""),
                        "节": point.get("节", ""),
                        "chapter_order": point.get("chapter_order", 0),
                        "一级知识点": point.get("一级知识点", ""),
                        "一级知识点id": "",
                        "二级知识点标题": point.get("二级知识点标题", ""),
                        "二级知识点详情": point.get("二级知识点详情", ""),
                        "备注": "需新增",
                        "_confidence": 0,
                        "_new_title": "",
                        "_new_detail": "",
                        "_kb_chapter": "",
                        "_kb_section": "",
                    })

            ai_match = sum(1 for it in parsed_map.values() if it.get("matched"))
            log_fn(f"  批次 {i//batch_size+1}: {len(batch)}条 → AI匹配{ai_match}条")

        except Exception as e:
            log_fn(f"  ⚠️ 批次 {i//batch_size+1} 失败: {str(e)[:80]}", "error")
            for point in batch:
                results.append({
                    "章": point.get("章", ""),
                    "节": point.get("节", ""),
                    "chapter_order": point.get("chapter_order", 0),
                    "一级知识点": point.get("一级知识点", ""),
                    "一级知识点id": "",
                    "二级知识点标题": point.get("二级知识点标题", ""),
                    "二级知识点详情": point.get("二级知识点详情", ""),
                    "备注": "需新增（AI比对失败）",
                    "_confidence": 0,
                    "_new_title": "",
                    "_new_detail": "",
                    "_kb_chapter": "",
                    "_kb_section": "",
                })

    ai_exist = sum(1 for r in results if "已存在" in r.get("备注", ""))
    log_fn(f"✅ 第二轮完成：AI匹配 {ai_exist} 条，需新增 {len(results)-ai_exist} 条", "success")
    return results


# ============================================================
# LLM API调用（带重试）
# ============================================================
def detect_api_format(api_url):
    """根据API地址自动判断接口格式"""
    url_lower = api_url.lower()
    if "anthropic.com" in url_lower or "/v1/messages" in url_lower:
        return "claude"
    if "generativelanguage.googleapis.com" in url_lower:
        return "gemini"
    # 其余都走OpenAI兼容格式（通义/DeepSeek/智谱/Moonshot/OpenAI等）
    return "openai"


def call_llm_api(api_config, user_prompt, system_prompt=None, max_retries=3):
    url = api_config["api_url"]
    key = api_config["api_key"]
    model = api_config["model"]
    fmt = detect_api_format(url)

    # 根据格式构建请求
    if fmt == "claude":
        body_dict = {
            "model": model,
            "max_tokens": 16000,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        if system_prompt:
            body_dict["system"] = system_prompt
        headers = {
            "Content-Type": "application/json",
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
        }
    elif fmt == "gemini":
        # Gemini REST API: POST .../models/{model}:generateContent?key=KEY
        # 把system和user拼在一起（Gemini的system_instruction格式）
        contents = []
        if system_prompt:
            contents.append({"role": "user", "parts": [{"text": system_prompt + "\n\n" + user_prompt}]})
        else:
            contents.append({"role": "user", "parts": [{"text": user_prompt}]})
        body_dict = {
            "contents": contents,
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 16000},
        }
        # Gemini的URL格式: .../models/gemini-xxx:generateContent
        if ":generateContent" not in url:
            url = f"{url.rstrip('/')}/models/{model}:generateContent"
        if "?" in url:
            url += f"&key={key}"
        else:
            url += f"?key={key}"
        headers = {"Content-Type": "application/json"}
    else:
        # OpenAI兼容格式
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        body_dict = {
            "model": model,
            "messages": messages,
            "max_tokens": 16000,
            "temperature": 0.1,
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        }

    data = json.dumps(body_dict).encode("utf-8")

    last_error = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=300) as resp:
                result = json.loads(resp.read().decode("utf-8"))

            # 解析各格式的返回值
            if fmt == "claude":
                # Anthropic: {"content": [{"type":"text","text":"..."}]}
                content = result.get("content", [])
                texts = [c.get("text", "") for c in content if c.get("type") == "text"]
                return "\n".join(texts)
            elif fmt == "gemini":
                # Gemini: {"candidates":[{"content":{"parts":[{"text":"..."}]}}]}
                candidates = result.get("candidates", [])
                if candidates:
                    parts = candidates[0].get("content", {}).get("parts", [])
                    return "\n".join(p.get("text", "") for p in parts)
                return ""
            else:
                # OpenAI兼容
                if "choices" in result and result["choices"]:
                    return result["choices"][0].get("message", {}).get("content", "")
                elif "output" in result:
                    out = result["output"]
                    if isinstance(out, str):
                        return out
                    return (out.get("text", "") or
                            out.get("choices", [{}])[0].get("message", {}).get("content", ""))
                elif "result" in result:
                    return result["result"]
                return ""

        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 3)
    raise last_error


def parse_json_response(text):
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    # 1. Try direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # 2. Try extracting complete JSON array
    match = re.search(r"\[[\s\S]*\]", cleaned)
    if match:
        try:
            return json.loads(match.group(0))
        except:
            pass

    # 3. Try extracting single JSON object
    match = re.search(r"\{[\s\S]*\}", cleaned)
    if match:
        try:
            obj = json.loads(match.group(0))
            return [obj] if isinstance(obj, dict) else obj
        except:
            pass

    # 4. Truncated JSON repair: find all complete {...} objects in a broken array
    objects = []
    for m in re.finditer(r'\{[^{}]*\}', cleaned):
        try:
            obj = json.loads(m.group(0))
            if isinstance(obj, dict) and ("二级知识点标题" in obj or "一级知识点" in obj):
                objects.append(obj)
        except:
            pass
    if objects:
        return objects

    raise ValueError(f"无法解析AI返回的JSON: {cleaned[:300]}")


# ============================================================
# 知识点梳理Prompt
# ============================================================
def build_extraction_prompt(ref_samples, chunk):
    ref_text = ""
    if ref_samples:
        ref_text = "\n".join([
            f"  章:{r['章']} | 节:{r['节']} | 一级知识点:{r['一级知识点']} | 二级标题:{r['二级知识点标题']} | 二级详情:{r['二级知识点详情'][:120]}"
            for r in ref_samples[:12]
        ])

    chapter = chunk.get("chapter", "")
    section = chunk.get("section", "")

    system = f"""你是K12教材知识点梳理专家。严格按规则梳理：

## 规则
1. **仅处理正式课文知识性内容**。跳过：活动课、跨学科学习、附录、年表、单元导语、章首引导文字、课后材料、阅读卡片、图片说明、思考题、练习题、"活动"栏目。
2. **章节名称**用我提供的，不要自编。
3. **中等颗粒度**——不要太粗也不要太细。
4. **一级知识点**：归纳分类名称（如"北方地区地形特征""新中国的成立与巩固"）
5. **二级知识点**：一个一级知识点下可以有多个二级知识点。每个二级知识点包含：
   - 标题：用简短、标准化的名词短语（如"开国大典""土地改革""十一届三中全会"）
   - 详情：简洁、规范、教材化纯文本
6. **按主题分类**：相关的知识点归入同一个一级知识点下

{f"## 参考风格{chr(10)}{ref_text}" if ref_text else ""}

## 输出格式
JSON数组，每个元素代表一个一级知识点：
{{"章":"{chapter}","节":"{section}","一级知识点":"...","二级知识点列表":[{{"标题":"...","详情":"..."}},{{"标题":"...","详情":"..."}}]}}

只输出JSON数组。不要markdown代码块。"""

    user = f"""梳理以下教材内容的知识点：

章节：{chapter} {section}

正文：
{chunk['text']}

输出JSON数组。"""

    return system, user


# ============================================================
# 主处理流程
# ============================================================
def process_textbook(task_id, pdf_path, kb_path, ref_path, subject, api_config, log_fn, match_config=None):
    try:
        # Step 1: PDF
        log_fn("📄 第一步：提取PDF文字...", "info")
        pages = extract_pdf_text(pdf_path)
        total_chars = sum(len(t) for t in pages.values())
        log_fn(f"  共 {len(pages)} 页，{total_chars:,} 字符", "success")

        if total_chars < 100:
            log_fn("❌ PDF几乎没有文字，可能是扫描件。", "error")
            return None

        # Step 2: 章节分割
        log_fn("📑 第二步：解析目录、分割章节...", "info")
        toc_entries = parse_toc_entries(pages)

        if toc_entries:
            content_entries = [e for e in toc_entries if e["type"] != "skip"]
            skip_entries = [e for e in toc_entries if e["type"] == "skip"]
            log_fn(f"  目录: {len(content_entries)} 章节, {len(skip_entries)} 跳过项", "info")
            chunks = split_pages_by_toc(pages, toc_entries)
        else:
            log_fn("  未找到目录，用正则分割", "warn")
            chunks = split_pages_by_regex(pages)

        if not chunks:
            log_fn("❌ 未识别到章节内容", "error")
            return None

        for c in chunks:
            log_fn(f"  📌 {c['info'][:50]}  ({len(c['text'])}字)", "info")

        # Step 3: 旧库
        log_fn("🗄️ 第三步：解析旧库表格...", "info")
        all_kb = parse_old_kb(kb_path)
        log_fn(f"  旧库共 {len(all_kb):,} 条", "info")

        # 筛选：学科（必须）
        target_subject = normalize_subject(subject)
        filtered_kb = [r for r in all_kb if r["subject"] == target_subject]
        log_fn(f"  学科「{target_subject}」: {len(filtered_kb):,} 条", "success")

        # 筛选：学段、年级（可选）
        filters = match_config or {}
        for dim, field in [("period","period"),("grade","grade")]:
            fval = filters.get(dim, "")
            if fval and fval != "全部":
                before = len(filtered_kb)
                filtered_kb = [r for r in filtered_kb if r.get(field, "") == fval]
                log_fn(f"  筛选{dim}「{fval}」: {before} → {len(filtered_kb)} 条", "info")

        subject_kb = filtered_kb
        if not subject_kb:
            log_fn(f"  ⚠️ 筛选后无记录，所有知识点将标记为[需新增]", "warn")

        # Step 4: 参考表格
        ref_samples = []
        if ref_path:
            log_fn("📋 解析参考表格...", "info")
            ref_samples = parse_ref_table(ref_path)
            log_fn(f"  {len(ref_samples)} 条样例", "success")

        # Step 5: AI梳理（并行）
        log_fn("🤖 第四步：AI梳理知识点...", "info")
        parallel = match_config.get("parallel", 4) if match_config else 4
        log_fn(f"  共 {len(chunks)} 个章节，{parallel} 路并行处理...", "info")

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_one_chunk(idx, chunk):
            system_prompt, user_prompt = build_extraction_prompt(ref_samples, chunk)
            response = call_llm_api(api_config, user_prompt, system_prompt)
            hierarchical_points = parse_json_response(response)

            # 扁平化层级结构
            flat_points = []
            if isinstance(hierarchical_points, list):
                for primary in hierarchical_points:
                    primary_name = primary.get("一级知识点", "")
                    secondary_list = primary.get("二级知识点列表", [])

                    # 如果没有二级知识点列表，尝试兼容旧格式
                    if not secondary_list and primary.get("二级知识点标题"):
                        secondary_list = [{
                            "标题": primary.get("二级知识点标题", ""),
                            "详情": primary.get("二级知识点详情", "")
                        }]

                    for secondary in secondary_list:
                        flat_points.append({
                            "章": chunk["chapter"],  # 强制使用chunk中的章节信息，确保顺序正确
                            "节": chunk["section"],  # 强制使用chunk中的节信息
                            "chapter_order": chunk.get("chapter_order", 0),
                            "一级知识点": primary_name,
                            "二级知识点标题": secondary.get("标题", ""),
                            "二级知识点详情": secondary.get("详情", ""),
                        })
                return idx, flat_points, None
            return idx, [], "格式异常"

        all_new_points = []
        consecutive_fails = 0
        done_count = 0
        failed_early = False
        chunk_results = {}  # idx → points, preserve order

        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futures = {}
            for i, chunk in enumerate(chunks):
                futures[pool.submit(process_one_chunk, i, chunk)] = (i, chunk)

            for future in as_completed(futures):
                i, chunk = futures[future]
                done_count += 1
                try:
                    idx, points, err = future.result()
                    if err:
                        log_fn(f"  [{done_count}/{len(chunks)}] {chunk['info'][:40]} → ⚠️ {err}", "warn")
                    else:
                        chunk_results[idx] = points
                        log_fn(f"  [{done_count}/{len(chunks)}] {chunk['info'][:40]} → ✓ {len(points)} 条", "success")
                        consecutive_fails = 0
                except Exception as e:
                    log_fn(f"  [{done_count}/{len(chunks)}] {chunk['info'][:40]} → ✗ {str(e)[:80]}", "error")
                    consecutive_fails += 1
                    if consecutive_fails >= 3:
                        log_fn("", "error")
                        log_fn("🛑 连续3次失败，自动停止。请检查API配置。", "error")
                        failed_early = True
                        pool.shutdown(wait=False, cancel_futures=True)
                        break

        # Reassemble in original chapter order
        for idx in sorted(chunk_results.keys()):
            all_new_points.extend(chunk_results[idx])

        log_fn(f"  共提取 {len(all_new_points)} 条知识点", "success")

        if not all_new_points:
            log_fn("❌ 未提取到知识点。请检查API配置。", "error")
            return None

        # Step 6: 比对
        log_fn("🔗 第五步：与旧库比对去重...", "info")
        final_results = match_with_old_kb(all_new_points, subject_kb, api_config, log_fn, match_config)

        # Step 7: Excel
        log_fn("📊 第六步：生成Excel...", "info")
        output_name = f"{Path(pdf_path).stem}_知识点梳理_{time.strftime('%m%d_%H%M')}.xlsx"
        output_path = OUTPUT_DIR / output_name
        generate_excel(final_results, str(output_path))

        exist_count = sum(1 for r in final_results if "已存在" in r.get("备注", ""))
        new_count = sum(1 for r in final_results if "需新增" in r.get("备注", ""))
        log_fn(f"🎉 完成！共 {len(final_results)} 条：已存在 {exist_count}，需新增 {new_count}", "success")

        return {
            "filename": output_name,
            "total": len(final_results),
            "exist": exist_count,
            "new_count": new_count,
            "results": final_results,
        }

    except Exception as e:
        log_fn(f"❌ 异常: {traceback.format_exc()}", "error")
        return None


# ============================================================
# Excel生成
# ============================================================
def generate_excel(results, output_path):
    import openpyxl
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

    # 排序：按章节顺序排列
    def sort_key(r):
        return (
            r.get("chapter_order", 999),
            r.get("章", ""),
            r.get("节", ""),
            r.get("一级知识点", ""),
            r.get("二级知识点标题", "")
        )

    sorted_results = sorted(results, key=sort_key)

    wb = openpyxl.Workbook()

    # ==========================================
    # Sheet 1: 知识点梳理（干净的7列标准格式）
    # ==========================================
    ws = wb.active
    ws.title = "知识点梳理"

    headers = ["章", "节", "一级知识点", "一级知识点id", "二级知识点标题", "二级知识点详情", "备注"]
    hfill = PatternFill(start_color="2B5797", end_color="2B5797", fill_type="solid")
    hfont = Font(name="微软雅黑", size=11, bold=True, color="FFFFFF")
    border = Border(
        left=Side(style="thin", color="D0D0D0"), right=Side(style="thin", color="D0D0D0"),
        top=Side(style="thin", color="D0D0D0"), bottom=Side(style="thin", color="D0D0D0"),
    )
    dfont = Font(name="微软雅黑", size=10)

    for col, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=col, value=h)
        c.font = hfont; c.fill = hfill; c.border = border
        c.alignment = Alignment(horizontal="center", vertical="center")

    efill = PatternFill(start_color="F0FAF0", end_color="F0FAF0", fill_type="solid")
    nfill = PatternFill(start_color="FFFAF0", end_color="FFFAF0", fill_type="solid")

    for row_idx, r in enumerate(sorted_results, 2):
        is_exist = "已存在" in r.get("备注", "")
        vals = [r.get("章",""), r.get("节",""), r.get("一级知识点",""),
                r.get("一级知识点id",""), r.get("二级知识点标题",""),
                r.get("二级知识点详情",""), r.get("备注","")]
        fill = efill if is_exist else nfill
        for col, val in enumerate(vals, 1):
            c = ws.cell(row=row_idx, column=col, value=sanitize_excel_value(val))
            c.font = dfont; c.fill = fill; c.border = border
            c.alignment = Alignment(vertical="top", wrap_text=True)

    for cl, w in {"A":25,"B":25,"C":20,"D":15,"E":25,"F":50,"G":18}.items():
        ws.column_dimensions[cl].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:G{len(sorted_results)+1}"

    # ==========================================
    # Sheet 2: 对比详情（调试和复查用）
    # ==========================================
    ws2 = wb.create_sheet("对比详情")
    dh = ["行号", "备注", "匹配置信度",
          "旧库Kid", "旧库标题", "旧库详情(前80字)", "旧库篇章", "旧库模块",
          "新教材标题", "新教材详情(前80字)"]
    dhfill = PatternFill(start_color="5B4A8A", end_color="5B4A8A", fill_type="solid")
    for col, h in enumerate(dh, 1):
        c = ws2.cell(row=1, column=col, value=h)
        c.font = hfont; c.fill = dhfill; c.border = border
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for ri, r in enumerate(sorted_results, 2):
        is_exist = "已存在" in r.get("备注", "")
        if not is_exist:
            continue
        conf = r.get("_confidence", 0)
        conf_txt = "AI判定" if conf == -1 else f"{conf}%"
        vals = [
            ri, r.get("备注",""), conf_txt,
            r.get("一级知识点id",""), r.get("二级知识点标题",""),
            (r.get("二级知识点详情","") or "")[:80],
            r.get("_kb_chapter",""), r.get("_kb_section",""),
            r.get("_new_title",""),
            (r.get("_new_detail","") or "")[:80],
        ]
        row_idx = ws2.max_row + 1
        for col, val in enumerate(vals, 1):
            c = ws2.cell(row=row_idx, column=col, value=sanitize_excel_value(val))
            c.font = dfont; c.border = border
            c.alignment = Alignment(vertical="top", wrap_text=True)

    for cl, w in {"A":5,"B":16,"C":10,"D":14,"E":22,"F":35,"G":20,"H":20,"I":22,"J":35}.items():
        ws2.column_dimensions[cl].width = w
    ws2.freeze_panes = "A2"

    # ==========================================
    # Sheet 3: 汇总说明
    # ==========================================
    ws3 = wb.create_sheet("汇总说明")
    ec = sum(1 for r in sorted_results if "已存在" in r.get("备注",""))
    nc = len(sorted_results) - ec
    cm = [r for r in sorted_results if r.get("_confidence",0) > 0]
    am = [r for r in sorted_results if r.get("_confidence",0) == -1]
    hc = sum(1 for r in cm if r["_confidence"] >= 80)
    mc = sum(1 for r in cm if 60 <= r["_confidence"] < 80)
    lc = sum(1 for r in cm if r["_confidence"] < 60)

    sm = [
        ["项目","数量","说明"],
        ["总条数",len(sorted_results),""],
        ["已存在",ec,"匹配到旧库，回填了Kid和内容"],
        ["需新增",nc,"旧库中没有，需要人工补充Kid"],
        ["","",""],
        ["匹配置信度分布","",""],
        ["高置信(>=80%)",hc,"标题高度匹配，可信赖"],
        ["中置信(60-79%)",mc,"有相似性，建议在「对比详情」sheet核实"],
        ["低置信(<60%)",lc,"匹配较勉强，建议核实"],
        ["AI语义匹配",len(am),"代码未匹配，AI判定相似"],
        ["","",""],
        ["说明","",""],
        ["","","「知识点梳理」sheet是干净的7列标准格式，可直接使用"],
        ["","","「对比详情」sheet列出了所有已匹配行的旧库原始信息，用于核实"],
        ["","","如果对匹配结果不满意，可以在网页上调整阈值后重跑"],
    ]

    cst = {}
    for r in sorted_results:
        ch = r.get("章","未知")
        if ch not in cst: cst[ch] = {"t":0,"e":0,"n":0}
        cst[ch]["t"] += 1
        if "已存在" in r.get("备注",""): cst[ch]["e"] += 1
        else: cst[ch]["n"] += 1
    sm.append(["","",""])
    sm.append(["按章统计","总/已存在/需新增",""])
    for ch, st in cst.items():
        sm.append([ch, f"{st['t']}/{st['e']}/{st['n']}", ""])

    for ri, rd in enumerate(sm, 1):
        for col, val in enumerate(rd, 1):
            c = ws3.cell(row=ri, column=col, value=sanitize_excel_value(val))
            if ri == 1 or rd[0] in ("匹配置信度分布","说明","按章统计"):
                c.font = hfont; c.fill = hfill
            else:
                c.font = dfont
            c.border = border
    ws3.column_dimensions["A"].width = 22
    ws3.column_dimensions["B"].width = 28
    ws3.column_dimensions["C"].width = 55
    wb.save(output_path)


# ============================================================
# 任务管理
# ============================================================
tasks = {}

def run_task(task_id, pdf_path, kb_path, ref_path, subject, api_config, match_config=None):
    task = tasks[task_id]
    task["status"] = "running"
    def log_fn(msg, msg_type="info"):
        task["logs"].append({"time": time.strftime("%H:%M:%S"), "msg": msg, "type": msg_type})
    result = process_textbook(task_id, pdf_path, kb_path, ref_path, subject, api_config, log_fn, match_config)
    task["status"] = "done" if result else "error"
    task["result"] = result


# ============================================================
# HTTP Server
# ============================================================
class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            h = get_frontend_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(h)))
            self.end_headers()
            self.wfile.write(h)
        elif path.startswith("/api/task/"):
            tid = path.split("/")[-1]
            t = tasks.get(tid)
            if t:
                self._json({"status": t["status"], "logs": t["logs"], "result": t.get("result")})
            else:
                self._json({"error": "任务不存在"}, 404)
        elif path.startswith("/api/download/"):
            fn = urllib.parse.unquote(path[len("/api/download/"):])
            fp = OUTPUT_DIR / fn
            if fp.exists():
                with open(fp, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{urllib.parse.quote(fn)}")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self._json({"error": "文件不存在"}, 404)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/api/upload":
            self._handle_upload()
        elif path == "/api/start":
            self._handle_start()
        elif path == "/api/detect-subjects":
            self._handle_detect_subjects()
        elif path == "/api/preview-pdf":
            self._handle_preview_pdf()
        else:
            self._json({"error": "未知接口"}, 404)

    def _read_body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", 0)))

    def _handle_upload(self):
        ct = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in ct:
            self._json({"error": "需要multipart/form-data"}, 400)
            return
        boundary = ct.split("boundary=")[-1].strip()
        body = self._read_body()

        files = {}
        for part in body.split(f"--{boundary}".encode()):
            if b"Content-Disposition" not in part:
                continue
            dm = re.search(rb'name="([^"]+)"', part)
            fm = re.search(rb'filename="([^"]+)"', part)
            if not dm:
                continue
            field = dm.group(1).decode("utf-8")
            he = part.find(b"\r\n\r\n")
            if he == -1:
                continue
            data = part[he + 4:]
            if data.endswith(b"\r\n"):
                data = data[:-2]
            if fm:
                orig = fm.group(1).decode("utf-8")
                safe = f"{uuid.uuid4().hex[:8]}_{orig}"
                sp = UPLOAD_DIR / safe
                with open(sp, "wb") as f:
                    f.write(data)
                files[field] = {"path": str(sp), "name": orig, "size": len(data)}
            else:
                files[field] = data.decode("utf-8")

        self._json({"files": files})

    def _handle_detect_subjects(self):
        body = json.loads(self._read_body())
        kp = body.get("kb_path", "")
        if not kp or not Path(kp).exists():
            self._json({"error": "文件不存在"}, 400)
            return
        try:
            filters = detect_kb_filters(kp)
            # Also return legacy subjects format for compatibility
            self._json({"subjects": filters.get("subjects", []), "filters": filters})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_preview_pdf(self):
        body = json.loads(self._read_body())
        pp = body.get("pdf_path", "")
        if not pp or not Path(pp).exists():
            self._json({"error": "文件不存在"}, 400)
            return
        try:
            pages = extract_pdf_text(pp)
            toc = parse_toc_entries(pages)
            chunks = split_pages_by_toc(pages, toc) if toc else split_pages_by_regex(pages)
            self._json({
                "total_pages": len(pages),
                "total_chars": sum(len(t) for t in pages.values()),
                "toc_entries": len(toc),
                "chunks": [{"info": c["info"][:60], "chars": len(c["text"]),
                            "preview": c["text"][:200]} for c in chunks],
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_start(self):
        body = json.loads(self._read_body())
        pp, kp, rp = body.get("pdf_path"), body.get("kb_path"), body.get("ref_path")
        subj = body.get("subject", "")
        ac = {k: body.get(k, "") for k in ("api_url", "api_key", "model")}
        mc = {
            "threshold": int(body.get("threshold", 70)),
            "w_title": int(body.get("w_title", 50)),
            "w_detail": int(body.get("w_detail", 40)),
            "w_k1": int(body.get("w_k1", 10)),
            "use_ai": body.get("use_ai", True),
            "parallel": int(body.get("parallel", 4)),
            "period": body.get("period", ""),
            "grade": body.get("grade", ""),
        }

        if not pp or not kp:
            self._json({"error": "缺少必要文件"}, 400)
            return
        if not ac["api_key"]:
            self._json({"error": "缺少API Key"}, 400)
            return

        tid = uuid.uuid4().hex[:12]
        tasks[tid] = {"status": "pending", "logs": [], "result": None}
        threading.Thread(target=run_task, args=(tid, pp, kp, rp, subj, ac, mc), daemon=True).start()
        self._json({"task_id": tid})


# ============================================================
# 前端HTML
# ============================================================
def get_frontend_html():
    return '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>教材知识点梳理工具</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#f5f0e8;--s1:#ffffff;--s2:#faf7f2;--bd:#e0d8cc;--bdh:#c8bfb0;--t:#2d2418;--td:#6b5d4f;--tm:#9a8d7f;--ac:#c96442;--gn:#3a8c6e;--yw:#c48a2a;--rd:#c44242;--r:12px}
body{font-family:system-ui,-apple-system,"Segoe UI","Microsoft YaHei",sans-serif;background:var(--bg);color:var(--t);min-height:100vh}
.wrap{max-width:880px;margin:0 auto;padding:20px 16px 60px}
h1{text-align:center;font-size:26px;font-weight:700;color:var(--ac);padding:32px 0 6px}
.sub{text-align:center;color:var(--td);font-size:13px;margin-bottom:28px}
.steps{display:flex;justify-content:center;gap:6px;margin-bottom:28px}
.st{display:flex;align-items:center;gap:5px;font-size:12px;color:var(--tm)}.st.on{color:var(--ac)}.st.ok{color:var(--gn)}
.dot{width:24px;height:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;border:2px solid var(--bd);transition:.2s}
.st.on .dot{border-color:var(--ac);background:rgba(201,100,66,.08);color:var(--ac)}
.st.ok .dot{border-color:var(--gn);background:rgba(58,140,110,.08);color:var(--gn)}
.st+.st::before{content:"";width:28px;height:1px;background:var(--bd);margin-right:6px}
.card{background:var(--s1);border:1px solid var(--bd);border-radius:var(--r);padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.card-t{font-size:14px;font-weight:600;margin-bottom:14px;display:flex;align-items:center;gap:7px;color:var(--t)}
.upg{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:14px}@media(max-width:640px){.upg{grid-template-columns:1fr}}
.upz{background:var(--s2);border:2px dashed var(--bd);border-radius:var(--r);padding:24px 12px;text-align:center;cursor:pointer;transition:.2s}
.upz:hover{border-color:var(--bdh);background:#fff}.upz.ok{border-color:var(--gn);border-style:solid;background:rgba(58,140,110,.03)}
.upz .ic{font-size:28px;margin-bottom:8px}.upz .lb{font-size:13px;font-weight:500;margin-bottom:4px;color:var(--t)}
.upz .ht{font-size:11px;color:var(--tm)}.upz .fn{font-size:11px;color:var(--gn);background:rgba(58,140,110,.06);padding:4px 8px;border-radius:6px;margin-top:6px;word-break:break-all}
.fr{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px}.fr.f1{grid-template-columns:1fr}
.fg{display:flex;flex-direction:column;gap:3px}
.fl{font-size:11px;color:var(--td)}
.fi{background:var(--s2);border:1px solid var(--bd);border-radius:8px;padding:9px 11px;color:var(--t);font-size:13px;font-family:inherit;outline:0;transition:.2s;width:100%}
.fi:focus{border-color:var(--ac);box-shadow:0 0 0 2px rgba(201,100,66,.1)}.fi::placeholder{color:var(--tm)}
.pre{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px}
.pb{padding:5px 12px;border-radius:18px;border:1px solid var(--bd);background:var(--s2);color:var(--td);font-size:12px;cursor:pointer;transition:.2s;font-family:inherit}
.pb:hover{border-color:var(--ac);color:var(--ac)}.pb.on{border-color:var(--ac);background:rgba(201,100,66,.08);color:var(--ac)}
.btns{display:flex;justify-content:center;gap:10px;margin:20px 0}
.btn{padding:10px 28px;border-radius:8px;font-size:14px;font-weight:500;cursor:pointer;border:0;transition:.2s;font-family:inherit}
.btn:disabled{opacity:.35;cursor:not-allowed}
.bp{background:var(--ac);color:#fff}.bp:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 4px 16px rgba(201,100,66,.25);background:#b85838}
.bs{background:var(--s2);color:var(--t);border:1px solid var(--bd)}
.bg{background:var(--gn);color:#fff}
.pbar{width:100%;height:5px;background:var(--bg);border-radius:3px;overflow:hidden;margin-bottom:6px}
.pfill{height:100%;background:linear-gradient(90deg,var(--ac),#d4845a);border-radius:3px;transition:width .4s}
.ptxt{font-size:12px;color:var(--td);text-align:center}
.logs{background:var(--s2);border:1px solid var(--bd);border-radius:8px;padding:10px;max-height:300px;overflow-y:auto;font-family:Menlo,Consolas,"Courier New",monospace;font-size:11px;line-height:1.8}
.logs::-webkit-scrollbar{width:3px}.logs::-webkit-scrollbar-thumb{background:var(--bd);border-radius:2px}
.le{display:flex;gap:8px}.lt{color:var(--tm);flex-shrink:0}
.le.info .lm{color:var(--td)}.le.success .lm{color:var(--gn)}.le.warn .lm{color:var(--yw)}.le.error .lm{color:var(--rd)}
.rc{text-align:center;padding:24px}.rc .em{font-size:44px;margin-bottom:10px}.rc .tt{font-size:17px;font-weight:600;margin-bottom:4px}
.rc .ds{font-size:13px;color:var(--td);margin-bottom:14px}
.stats{display:flex;justify-content:center;gap:28px;margin-bottom:18px}
.si{text-align:center}.sn{font-size:26px;font-weight:700}.sl{font-size:11px;color:var(--td);margin-top:1px}
.sn.a{color:var(--ac)}.sn.g{color:var(--gn)}.sn.y{color:var(--yw)}
.tw{overflow:hidden;border-radius:var(--r);box-shadow:0 1px 3px rgba(0,0,0,.04)}
.th{padding:12px 16px;font-size:13px;font-weight:500;border-bottom:1px solid var(--bd);display:flex;justify-content:space-between;background:var(--s2)}
.ts{overflow:auto;max-height:400px}.ts::-webkit-scrollbar{height:3px;width:3px}.ts::-webkit-scrollbar-thumb{background:var(--bd)}
table{width:100%;border-collapse:collapse;font-size:11px}
th{position:sticky;top:0;background:var(--s2);padding:8px 10px;text-align:left;font-weight:500;color:var(--td);white-space:nowrap;border-bottom:1px solid var(--bd);z-index:1}
td{padding:6px 10px;border-bottom:1px solid var(--bd);max-width:220px;overflow:hidden;text-overflow:ellipsis}
tr:hover td{background:rgba(201,100,66,.02)}
.te{display:inline-block;padding:1px 7px;border-radius:9px;font-size:10px}
.te.ex{background:rgba(58,140,110,.08);color:var(--gn)}.te.nw{background:rgba(196,138,42,.08);color:var(--yw)}.te.er{background:rgba(196,66,66,.08);color:var(--rd)}
.note{background:rgba(201,100,66,.04);border:1px solid rgba(201,100,66,.12);border-radius:8px;padding:10px 14px;font-size:12px;color:var(--td);margin-bottom:14px;line-height:1.6}
.note b{color:var(--ac);font-weight:500}
.err{background:rgba(196,66,66,.05);border:1px solid rgba(196,66,66,.15);border-radius:8px;padding:12px;color:var(--rd);font-size:12px;margin-bottom:12px}
.prev{background:var(--s2);border:1px solid var(--bd);border-radius:8px;padding:14px;margin-bottom:14px}
.prev-h{font-size:12px;font-weight:500;color:var(--ac);margin-bottom:8px}
.prev-item{display:flex;justify-content:space-between;padding:4px 0;font-size:11px;border-bottom:1px solid rgba(224,216,204,.6)}
.prev-item:last-child{border:0}
.prev-item .pi-n{color:var(--t);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.prev-item .pi-c{color:var(--tm);margin-left:8px;flex-shrink:0}
.spin{display:inline-block;width:14px;height:14px;border:2px solid rgba(201,100,66,.15);border-top-color:var(--ac);border-radius:50%;animation:sp .6s linear infinite;margin-right:6px;vertical-align:middle}
@keyframes sp{to{transform:rotate(360deg)}}
.fade{animation:fi .3s ease}@keyframes fi{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.stags{display:flex;gap:5px;flex-wrap:wrap;margin-top:6px}
.stag{padding:2px 9px;border-radius:12px;font-size:11px;border:1px solid var(--bd);color:var(--td);cursor:pointer;transition:.2s;background:var(--s2)}
.stag:hover{border-color:var(--ac);color:var(--ac)}.stag.on{border-color:var(--ac);background:rgba(201,100,66,.08);color:var(--ac)}
</style>
</head>
<body>
<div class="wrap">
<h1>📚 教材知识点梳理工具</h1>
<p class="sub">上传教材PDF → AI自动梳理知识点 → 与旧库去重比对 → 输出标准Excel</p>
<div class="steps" id="stepBar"></div>
<div id="content"></div>
</div>
<script>
const S={step:0,files:{pdf:null,kb:null,ref:null},fp:{},cachedPaths:null,preset:"claude",
apiUrl:"https://api.anthropic.com/v1/messages",
apiKey:"",model:"claude-sonnet-4-6",subject:"",subjects:[],
threshold:70,w_title:50,w_detail:40,w_k1:10,use_ai:true,parallel:4,period:"",grade:"",kbFilters:null,
taskId:null,logs:[],result:null,error:null,polling:null,pdfPrev:null,prevLoading:false};

const P={claude:{n:"Claude",u:"https://api.anthropic.com/v1/messages",m:"claude-sonnet-4-6"},
tongyi:{n:"通义千问",u:"https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",m:"qwen-plus"},
deepseek:{n:"DeepSeek",u:"https://api.deepseek.com/v1/chat/completions",m:"deepseek-chat"},
doubao:{n:"豆包",u:"https://ark.cn-beijing.volces.com/api/v3/chat/completions",m:""},
gemini:{n:"Gemini",u:"https://generativelanguage.googleapis.com/v1beta",m:"gemini-3-flash-preview"},
openai:{n:"OpenAI",u:"https://api.openai.com/v1/chat/completions",m:"gpt-5.4"},
zhipu:{n:"智谱AI",u:"https://open.bigmodel.cn/api/paas/v4/chat/completions",m:"glm-4-plus"},
moonshot:{n:"Moonshot",u:"https://api.moonshot.cn/v1/chat/completions",m:"moonshot-v1-128k"},
custom:{n:"自定义",u:"",m:""}};

const ST=["上传文件","配置API","处理中","完成"];
const E=s=>(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");

function R(){
  document.getElementById("stepBar").innerHTML=ST.map((s,i)=>{
    const c=i<S.step?"st ok":i===S.step?"st on":"st";
    return`<div class="${c}"><div class="dot">${i<S.step?"✓":i+1}</div><span>${s}</span></div>`;
  }).join("");
  const el=document.getElementById("content");
  el.innerHTML=[r0,r1,r2,r3][S.step]();bindEv();
}

function r0(){
  const f=(k,ic,lb,ht,ac,req)=>{const fi=S.files[k];
    return`<div class="upz ${fi?"ok":""}" data-u="${k}"><div class="ic">${ic}</div><div class="lb">${lb}${req?"":"（可选）"}</div>
    ${fi?`<div class="fn">✓ ${E(fi.name)} (${(fi.size/1024).toFixed(0)}KB)</div>`:`<div class="ht">${ht}</div>`}
    <input type="file" accept="${ac}" style="display:none" data-f="${k}"></div>`;};
  let pv="";
  if(S.prevLoading)pv=`<div class="prev"><div class="prev-h"><span class="spin"></span>分析PDF中...</div></div>`;
  else if(S.pdfPrev){const p=S.pdfPrev;
    pv=`<div class="prev"><div class="prev-h">📄 PDF: ${p.total_pages}页 / ${p.total_chars.toLocaleString()}字 / ${p.chunks.length}个章节</div>
    ${p.chunks.map(c=>`<div class="prev-item"><span class="pi-n">${E(c.info)}</span><span class="pi-c">${c.chars.toLocaleString()}字</span></div>`).join("")}</div>`;}
  return`<div class="fade"><div class="upg">
    ${f("pdf","📖","教材PDF","电子版 .pdf（必须）",".pdf",1)}
    ${f("kb","🗄️","旧库表格","已有知识点库（必须）",".xlsx,.xls",1)}
    ${f("ref","📋","参考表格","输出风格参考",".xlsx,.xls",0)}
  </div>${pv}
  <div class="note"><b>说明：</b>PDF需电子版（能复制文字）。旧库支持「九科知识卡详情」和「梳理样例」格式。参考表格可选。</div>
  <div class="btns"><button class="btn bp" ${S.files.pdf&&S.files.kb?"":"disabled"} onclick="goStep(1)">下一步：配置API →</button></div></div>`;
}

function r1(){
  const pb=Object.entries(P).map(([k,v])=>`<button class="pb ${S.preset===k?"on":""}" onclick="setPre('${k}')">${v.n}</button>`).join("");
  const st=S.subjects.length?`<div class="stags">${S.subjects.map(([n,c])=>`<span class="stag ${S.subject===n?"on":""}" onclick="pickSubj('${n}')">${n} (${c.toLocaleString()})</span>`).join("")}</div>`:"";
  return`<div class="fade"><div class="card"><div class="card-t">⚙️ 大模型API配置</div>
  <div class="pre">${pb}</div>
  <div class="fr f1"><div class="fg"><span class="fl">API地址</span><input class="fi" id="iU" value="${E(S.apiUrl)}" placeholder="https://..."></div></div>
  <div class="fr"><div class="fg"><span class="fl">API Key</span><input class="fi" id="iK" type="password" value="${E(S.apiKey)}" placeholder="sk-..."></div>
  <div class="fg"><span class="fl">模型名称</span><input class="fi" id="iM" value="${E(S.model)}" placeholder="如 qwen-plus"></div></div>
  <div class="fr"><div class="fg"><span class="fl">教材学科</span><input class="fi" id="iS" value="${E(S.subject)}" placeholder="如：地理">${st}</div>
  <div class="fg"><span class="fl">&nbsp;</span><span style="font-size:11px;color:var(--tm);padding:9px 0">点击标签选择或手动输入</span></div></div>
  ${S.kbFilters?`<div style="margin-top:6px"><span class="fl" style="display:block;margin-bottom:4px">旧库筛选（缩小比对范围，更快更准）</span>
  <div class="fr">
    <div class="fg"><span class="fl">学段</span><select class="fi" id="fPd"><option value="">全部</option>${(S.kbFilters.periods||[]).map(([v,c])=>'<option value="'+E(v)+'"'+(S.period===v?' selected':'')+'>'+E(v)+' ('+c+')</option>').join("")}</select></div>
    <div class="fg"><span class="fl">年级</span><select class="fi" id="fGr"><option value="">全部</option>${(S.kbFilters.grades||[]).map(([v,c])=>'<option value="'+E(v)+'"'+(S.grade===v?' selected':'')+'>'+E(v)+' ('+c+')</option>').join("")}</select></div>
  </div></div>`:""}</div>
  <div class="card"><div class="card-t">🔗 旧库匹配参数</div>
  <div class="fr">
    <div class="fg"><span class="fl">匹配阈值: <b id="vTh">${S.threshold}%</b>（越高越严格，越不容易匹配错）</span>
      <input type="range" min="40" max="95" value="${S.threshold}" id="iTh" oninput="document.getElementById('vTh').textContent=this.value+'%'" style="width:100%;accent-color:var(--ac)"></div>
    <div class="fg"><span class="fl">AI二轮比对</span>
      <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--td);padding:8px 0;cursor:pointer">
        <input type="checkbox" id="iAi" ${S.use_ai?"checked":""} style="accent-color:var(--ac)"> 代码未匹配的交给AI再判一轮
      </label></div>
  </div>
  <div class="fr">
    <div class="fg"><span class="fl">并行处理数: <b id="vPl">${S.parallel}</b>（同时处理几个章节，越大越快但API可能限流）</span>
      <input type="range" min="1" max="8" value="${S.parallel}" id="iPl" oninput="document.getElementById('vPl').textContent=this.value" style="width:100%;accent-color:var(--ac)"></div>
    <div class="fg"></div>
  </div>
  <details style="margin-top:6px"><summary style="cursor:pointer;font-size:11px;color:var(--tm)">高级：调整权重</summary>
  <div style="font-size:11px;color:var(--tm);padding:8px 0 4px">每个滑杆对应一个比对字段，权重越高该字段越重要</div>
  <div class="fr" style="margin-top:4px">
    <div class="fg"><span class="fl">二级标题匹配: <b id="vWt">${S.w_title}%</b></span>
      <input type="range" min="20" max="80" value="${S.w_title}" id="iWt" oninput="document.getElementById('vWt').textContent=this.value+'%'" style="width:100%;accent-color:var(--ac)">
      <span style="font-size:10px;color:var(--tm)">新教材二级标题 vs 旧库二级标题</span></div>
    <div class="fg"><span class="fl">二级详情匹配: <b id="vWd">${S.w_detail}%</b></span>
      <input type="range" min="10" max="60" value="${S.w_detail}" id="iWd" oninput="document.getElementById('vWd').textContent=this.value+'%'" style="width:100%;accent-color:var(--ac)">
      <span style="font-size:10px;color:var(--tm)">新教材详情前80字 vs 旧库详情前150字</span></div>
  </div>
  <div class="fr">
    <div class="fg"><span class="fl">一级知识点匹配: <b id="vW1">${S.w_k1}%</b></span>
      <input type="range" min="0" max="30" value="${S.w_k1}" id="iW1" oninput="document.getElementById('vW1').textContent=this.value+'%'" style="width:100%;accent-color:var(--ac)">
      <span style="font-size:10px;color:var(--tm)">新教材一级名称 vs 旧库一级名称</span></div>
    <div class="fg"></div>
  </div>
  </details></div>
  <div class="note"><b>推荐：</b>豆包 seed2.0-lite（便宜好用）、Claude claude-sonnet-4-6、OpenAI gpt-5.4、Gemini gemini-3-flash-preview<br><b>豆包：</b>模型名填你的接入点ID（如 ep-xxxx...），推荐创建 seed2.0-lite 的接入点<br><b>安全：</b>API Key仅本机使用。</div>
  ${S.error?`<div class="err">${E(S.error)}</div>`:""}
  <div class="btns"><button class="btn bs" onclick="goStep(0)">← 上一步</button><button class="btn bp" onclick="go()">🚀 开始处理</button></div></div>`;
}

function r2(){
  let pct=Math.min(95,S.logs.length*3.5);
  if(S.logs.some(l=>l.msg.includes("第六步")))pct=95;
  else if(S.logs.some(l=>l.msg.includes("第五步")))pct=80;
  else if(S.logs.some(l=>l.msg.includes("第四步")))pct=30;
  const lg=S.logs.map(l=>`<div class="le ${l.type}"><span class="lt">${l.time}</span><span class="lm">${E(l.msg)}</span></div>`).join("");
  return`<div class="fade"><div class="card"><div class="pbar"><div class="pfill" style="width:${pct}%"></div></div>
  <div class="ptxt">${S.error?"⚠️ 出错":'<span class="spin"></span>处理中...'}</div></div>
  ${S.error?`<div class="err">❌ ${E(S.error)}</div>`:""}<div class="logs" id="lb">${lg}</div>
  ${S.error?`<div class="btns"><button class="btn bs" onclick="goStep(1)">← 返回修改API</button><button class="btn bp" onclick="go()">🔄 重新处理</button></div>`:""}</div>`;
}

function r3(){
  if(!S.result)return"";const r=S.result;
  const lg=S.logs.map(l=>`<div class="le ${l.type}"><span class="lt">${l.time}</span><span class="lm">${E(l.msg)}</span></div>`).join("");
  const rows=(r.results||[]).slice(0,150).map(w=>{
    const rm=w["备注"]||"";
    const tg=rm.includes("已存在")?'<span class="te ex">已存在</span>':rm.includes("失败")?'<span class="te er">失败</span>':'<span class="te nw">需新增</span>';
    const d=w["二级知识点详情"]||"";
    return`<tr><td title="${E(w["章"]||"")}">${E((w["章"]||""))}</td><td>${E(w["节"]||"")}</td>
    <td>${E(w["一级知识点"]||"")}</td><td style="font-family:monospace;font-size:10px">${E(w["一级知识点id"]||"-")}</td>
    <td>${E(w["二级知识点标题"]||"")}</td><td title="${E(d)}">${E(d.substring(0,80))}${d.length>80?"...":""}</td><td>${tg}</td></tr>`;
  }).join("");
  return`<div class="fade"><div class="card rc"><div class="em">✅</div><div class="tt">梳理完成！</div>
  <div class="ds">${E(r.filename)}</div>
  <div class="stats"><div class="si"><div class="sn a">${r.total}</div><div class="sl">总数</div></div>
  <div class="si"><div class="sn g">${r.exist}</div><div class="sl">已存在</div></div>
  <div class="si"><div class="sn y">${r.total-r.exist}</div><div class="sl">需新增</div></div></div>
  <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap">
  <a class="btn bg" href="/api/download/${encodeURIComponent(r.filename)}" style="text-decoration:none">📥 下载Excel</a>
  <button class="btn bs" onclick="reset()">🔄 下一本</button></div>
  <div style="font-size:11px;color:var(--tm);text-align:center;margin-top:8px">提示：处理下一本前，建议先将新增知识点入库，再导出最新旧库上传</div></div>
  <div class="card tw"><div class="th"><span>预览</span><span style="font-size:11px;color:var(--tm)">共${r.total}条${r.total>150?"（前150）":""}</span></div>
  <div class="ts"><table><thead><tr><th>章</th><th>节</th><th>一级知识点</th><th>ID</th><th>二级标题</th><th>二级详情</th><th>备注</th></tr></thead>
  <tbody>${rows}</tbody></table></div></div>
  <details style="margin-top:12px"><summary style="cursor:pointer;font-size:12px;color:var(--td)">查看日志</summary>
  <div class="logs" style="margin-top:8px">${lg}</div></details></div>`;
}

function bindEv(){document.querySelectorAll("[data-u]").forEach(el=>{
  const k=el.dataset.u,inp=el.querySelector("input[type=file]");
  el.onclick=e=>{if(e.target!==inp)inp.click()};
  inp.onchange=()=>{if(inp.files[0]){S.files[k]=inp.files[0];if(k==="pdf")upPdf();if(k==="kb")upKb();R()}}
})}

async function upFile(file,field){
  const fd=new FormData();fd.append(field,file);
  const r=await fetch("/api/upload",{method:"POST",body:fd});
  const d=await r.json();
  if(d.files&&d.files[field]){S.fp[field]=d.files[field].path;return d.files[field].path}return null;
}
async function upPdf(){S.prevLoading=true;S.pdfPrev=null;R();
  try{const p=await upFile(S.files.pdf,"pdf");if(!p)return;
    const r=await fetch("/api/preview-pdf",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({pdf_path:p})});
    S.pdfPrev=await r.json();}catch(e){console.error(e)}S.prevLoading=false;R();}
async function upKb(){try{const p=await upFile(S.files.kb,"kb");if(!p)return;
    const r=await fetch("/api/detect-subjects",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({kb_path:p})});
    const d=await r.json();
    if(d.subjects){S.subjects=d.subjects;if(!S.subject&&d.subjects.length)S.subject=d.subjects[0][0]}
    if(d.filters)S.kbFilters=d.filters;
  }catch(e){console.error(e)}}

function goStep(n){S.step=n;S.error=null;R()}
function setPre(k){S.preset=k;if(k!=="custom"){S.apiUrl=P[k].u;S.model=P[k].m}R()}
function pickSubj(n){S.subject=n;const el=document.getElementById("iS");if(el)el.value=n;R()}

function rf(){const g=id=>document.getElementById(id);
  if(g("iU"))S.apiUrl=g("iU").value.trim();if(g("iK"))S.apiKey=g("iK").value.trim();
  if(g("iM"))S.model=g("iM").value.trim();if(g("iS"))S.subject=g("iS").value.trim();
  if(g("iTh"))S.threshold=parseInt(g("iTh").value);
  if(g("iWt"))S.w_title=parseInt(g("iWt").value);
  if(g("iWd"))S.w_detail=parseInt(g("iWd").value);
  if(g("iW1"))S.w_k1=parseInt(g("iW1").value);
  if(g("iAi"))S.use_ai=g("iAi").checked;
  if(g("iPl"))S.parallel=parseInt(g("iPl").value);
  if(g("fPd"))S.period=g("fPd").value;
  if(g("fGr"))S.grade=g("fGr").value;}

async function go(){rf();
  if(!S.apiUrl||!S.apiKey||!S.model){S.error="请填写完整API配置";R();return}
  if(!S.subject){S.error="请填写教材学科";R();return}
  S.step=2;S.logs=[];S.error=null;S.result=null;R();
  try{
    // Reuse cached paths if available (retry scenario)
    let pp=S.cachedPaths?.pp, kp=S.cachedPaths?.kp, rp=S.cachedPaths?.rp||"";
    if(!pp||!kp){
      const fd=new FormData();fd.append("pdf",S.files.pdf);fd.append("kb",S.files.kb);
      if(S.files.ref)fd.append("ref",S.files.ref);
      const ur=await(await fetch("/api/upload",{method:"POST",body:fd})).json();
      pp=ur.files?.pdf?.path;kp=ur.files?.kb?.path;rp=ur.files?.ref?.path||"";
      if(!pp||!kp)throw new Error("上传失败");
      S.cachedPaths={pp,kp,rp};
    }
    const sr=await(await fetch("/api/start",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({pdf_path:pp,kb_path:kp,ref_path:rp,subject:S.subject,api_url:S.apiUrl,api_key:S.apiKey,model:S.model,
        threshold:S.threshold,w_title:S.w_title,w_detail:S.w_detail,w_k1:S.w_k1,use_ai:S.use_ai,parallel:S.parallel,
        period:S.period,grade:S.grade})})).json();
    if(sr.error)throw new Error(sr.error);S.taskId=sr.task_id;
    S.polling=setInterval(async()=>{try{const d=await(await fetch(`/api/task/${S.taskId}`)).json();
      S.logs=d.logs||[];R();const lb=document.getElementById("lb");if(lb)lb.scrollTop=lb.scrollHeight;
      if(d.status==="done"){clearInterval(S.polling);S.result=d.result;S.step=3;R()}
      else if(d.status==="error"){clearInterval(S.polling);S.error="处理失败，查看日志";R()}}catch(e){}},1500);
  }catch(e){S.error=e.message;R()}}

function reset(){if(S.polling)clearInterval(S.polling);
  Object.assign(S,{step:0,files:{pdf:null,kb:null,ref:null},fp:{},cachedPaths:null,
    threshold:70,w_title:50,w_detail:40,w_k1:10,use_ai:true,parallel:4,period:"",grade:"",kbFilters:null,
    taskId:null,logs:[],result:null,error:null,pdfPrev:null,subjects:[]});R()}
R();
</script>
</body>
</html>'''


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """多线程HTTP服务器，解决浏览器并发请求时卡住的问题"""
    daemon_threads = True

def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"""
╔══════════════════════════════════════════════════╗
║   📚 教材知识点梳理与旧库去重工具 v2.0          ║
╠══════════════════════════════════════════════════╣
║                                                  ║
║   浏览器打开: http://localhost:{PORT}             ║
║                                                  ║
║   工作流:                                        ║
║   1. PDF文字提取 (pdfplumber)                    ║
║   2. 智能目录解析 + 章节分割                     ║
║   3. AI按章节梳理知识点                          ║
║   4. 代码精确匹配 + AI语义去重                   ║
║   5. 输出标准Excel                               ║
║                                                  ║
║   按 Ctrl+C 停止                                 ║
╚══════════════════════════════════════════════════╝
""")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
        server.server_close()

if __name__ == "__main__":
    main()
