# -*- coding: utf-8 -*-
"""상장폐지 체크 — 리스크 공시 수집기

DART 공시 중 개인 투자자에게 '악재'로 읽히는 유형만 추려 정적 JSON으로 굽는다.
앱은 이 JSON만 읽는다(앱이 DART를 직접 호출하지 않음) → 사용자가 늘어도 API 호출이 늘지 않는다.

법적 근거
  - DART 오픈API 이용약관: 상업 이용 금지 조항 없음, "개인·기업·기관 누구든지 이용 가능" 명시
  - ⚠️ 제23조 정확성·완전성 미보장 → 앱에 면책 문구 + 원문 링크 필수
  - ⚠️ 유사투자자문 경계: 이 파일은 '판단'을 만들지 않는다. 공시 사실만 분류·전달한다.

사용:
  python crawl_risk.py            # 최근 RECENT_DAYS 재수집 후 기존과 병합
  python crawl_risk.py --full     # WINDOW_DAYS 전체 재수집
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "data")
OUT_PATH = os.path.join(DATA_DIR, "signals.json")

API = "https://opendart.fss.or.kr/api"

WINDOW_DAYS = 180   # 앱이 보여줄 기간
RECENT_DAYS = 10    # 평소 실행 시 다시 훑을 기간([기재정정]이 늦게 붙는 걸 잡으려고 넉넉히)


# ── 리스크 분류 ────────────────────────────────────────────────
# key = 앱에서 쓸 코드, label = 사용자에게 보일 중립적 유형명
# ⚠️ "위험도"가 아니라 "유형"이다. 등급을 매기면 그 순간 투자판단이 된다.
# ⚠️ 순서가 곧 우선순위다. 위에서부터 먼저 매칭된다.
#    상폐 계열을 맨 위에 두는 이유: 한 공시가 여러 키워드에 걸릴 때
#    "상장폐지 사유 발생"이 "매매거래정지"보다 사용자에게 중요하다.
RISK_KINDS = [
    # 🔴 "매매거래정지"를 넣으면 안 된다.
    #    액면병합·감자 같은 절차적 정지가 대량으로 딸려온다(실측: 208+119+39건).
    #    "상장폐지 체크"에서 액면병합이 뜨면 그게 곧 앱을 못 믿게 만든다.
    #    상폐 사유로 인한 정지는 공시명에 "상장폐지"가 이미 들어 있어 아래 키워드로 잡힌다.
    ("delisting", "상폐", [
        "상장폐지",
        "상장적격성",
        "관리종목",
        "감사의견",
        "자본잠식",
    ]),
    ("finance",   "재무", [
        "부도발생",
        "회생절차",
        "파산신청",
        "채권은행",
    ]),
    ("operation", "경영", [
        "횡령",
        "배임",
        "영업정지",
        "소송등의제기",
    ]),
    ("overhang",  "오버행", [
        "전환사채권발행결정",
        "신주인수권부사채권발행결정",
        "교환사채권발행결정",
        "전환청구권행사",
    ]),
    ("dilution",  "희석", [
        "유상증자결정",
        "감자결정",
    ]),
]

CORRECTION_RE = re.compile(r"^\[[^\]]*정정\]")


def load_key():
    key = os.environ.get("DART_API_KEY", "")
    if not key:
        env_path = os.path.join(BASE, ".env")
        if os.path.exists(env_path):
            for line in open(env_path, encoding="utf-8"):
                if line.startswith("DART_API_KEY="):
                    key = line.split("=", 1)[1].strip()
    if not key:
        print("ERROR: DART_API_KEY not set", file=sys.stderr)
        sys.exit(1)
    return key


KEY = load_key()


def get_retry(url, params, tries=4):
    """일시 타임아웃 한 번에 수집이 통째로 죽지 않게 — 지수 백오프."""
    last = None
    for i in range(tries):
        try:
            return requests.get(url, params=params, timeout=40)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5 * (i + 1))
    raise RuntimeError(f"GET failed after {tries} tries: {url}: {last}")


def norm(s):
    """거래소공시는 공시명 안에 공백이 수십 개씩 박혀 온다.
    예: '기타시장안내              (상장폐지 관련)' → '기타시장안내 (상장폐지 관련)'"""
    return re.sub(r"\s+", " ", (s or "")).strip()


def classify(report_nm):
    """공시명 → (코드, 유형명). 리스크가 아니면 None."""
    plain = norm(CORRECTION_RE.sub("", report_nm))
    for code, label, keys in RISK_KINDS:
        if any(k in plain for k in keys):
            return code, label
    return None


def date_chunks(bgn_de, end_de, span_days=80):
    """DART는 corp_code 없이 조회하면 검색기간을 3개월로 제한한다(status=100).
    그래서 구간을 잘라서 여러 번 부른다. 80일이면 안전하다."""
    b = datetime.strptime(bgn_de, "%Y%m%d").date()
    e = datetime.strptime(end_de, "%Y%m%d").date()
    while b <= e:
        chunk_end = min(b + timedelta(days=span_days - 1), e)
        yield b.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")
        b = chunk_end + timedelta(days=1)


def fetch_type(bgn_de, end_de, pblntf_ty):
    """한 공시유형을 기간 전체(3개월 제한을 넘으면 분할)에 대해 훑는다."""
    out = []
    for cb, ce in date_chunks(bgn_de, end_de):
        out += _fetch_type_chunk(cb, ce, pblntf_ty)
    return out


def _fetch_type_chunk(bgn_de, end_de, pblntf_ty):
    out, page = [], 1
    while True:
        r = get_retry(f"{API}/list.json", {
            "crtfc_key": KEY, "bgn_de": bgn_de, "end_de": end_de,
            "pblntf_ty": pblntf_ty, "page_no": page, "page_count": 100,
        })
        d = r.json()
        status = d.get("status")
        if status == "013":          # 조회 결과 없음 — 정상
            break
        if status != "000":
            print(f"  [{pblntf_ty}] status={status} {d.get('message','')}", file=sys.stderr)
            break
        out += d.get("list", [])
        if page >= int(d.get("total_page", 1)):
            break
        page += 1
        time.sleep(0.15)             # 한도(일 2만건)엔 한참 못 미치지만 예의상
    return out


def collect(bgn_de, end_de):
    """B(주요사항보고) + I(거래소공시)에서 리스크 신호만 추린다.

    B: 유상증자·전환사채·감자·부도 등 (실측 2026-07 기준 하루 약 12건)
    I: 관리종목 지정·상장폐지 관련 (드물지만 가장 강한 신호라 같이 본다)
    """
    rows, seen = [], set()
    for ty in ("B", "I"):
        for it in fetch_type(bgn_de, end_de, ty):
            hit = classify(it.get("report_nm", ""))
            if not hit:
                continue
            rcept = it.get("rcept_no", "")
            if rcept in seen:
                continue
            seen.add(rcept)
            code, label = hit
            rows.append({
                "rcept_no":   rcept,
                "date":       it.get("rcept_dt", ""),
                "corp":       it.get("corp_name", ""),
                "corp_code":  it.get("corp_code", ""),
                "stock_code": (it.get("stock_code") or "").strip(),
                "kind":       code,
                "kind_label": label,
                "report":     norm(CORRECTION_RE.sub("", it.get("report_nm", ""))),
                "correction": bool(CORRECTION_RE.match(it.get("report_nm", ""))),
            })
    return rows


def load_prev():
    if not os.path.exists(OUT_PATH):
        return []
    try:
        return json.load(open(OUT_PATH, encoding="utf-8")).get("items", [])
    except Exception:  # noqa: BLE001
        return []


def main():
    full = "--full" in sys.argv
    today = datetime.now(KST).date()
    days = WINDOW_DAYS if full else RECENT_DAYS
    bgn = (today - timedelta(days=days)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    print(f"수집 구간: {bgn} ~ {end} ({'전체' if full else '증분'})")
    fresh = collect(bgn, end)
    print(f"  신규 수집 {len(fresh)}건")

    # 병합: rcept_no 기준으로 새로 받은 쪽이 이긴다([기재정정] 반영)
    merged = {r["rcept_no"]: r for r in load_prev()}
    merged.update({r["rcept_no"]: r for r in fresh})

    cutoff = (today - timedelta(days=WINDOW_DAYS)).strftime("%Y%m%d")
    items = [r for r in merged.values() if r["date"] >= cutoff]
    items.sort(key=lambda r: (r["date"], r["rcept_no"]), reverse=True)

    # 상장사만 남긴다 — 종목코드가 없으면 비상장이라 앱에서 볼 이유가 없다.
    listed = [r for r in items if r["stock_code"]]

    os.makedirs(DATA_DIR, exist_ok=True)
    payload = {
        "updated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        "window_days": WINDOW_DAYS,
        "source": "DART 전자공시 (opendart.fss.or.kr)",
        "notice": "공시 사실을 그대로 전달합니다. 투자 판단은 이용자 본인의 책임이며, 원문은 DART에서 확인하세요.",
        "count": len(listed),
        "items": listed,
    }
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    by_kind = {}
    for r in listed:
        by_kind[r["kind_label"]] = by_kind.get(r["kind_label"], 0) + 1
    print(f"  보관 {len(listed)}건 (상장사만, 최근 {WINDOW_DAYS}일)")
    for k, v in sorted(by_kind.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")
    print(f"  → {OUT_PATH}")


if __name__ == "__main__":
    main()
