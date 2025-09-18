import pandas as pd
import re
import unicodedata
from collections import OrderedDict, Counter
import traceback
from .config import STRICT_SCHEMA, REQUIRED_COLUMNS, QUARANTINE_DIR
import uuid, os, datetime as dt
import logging
logger = logging.getLogger("etl")

# ===============================================================================================
# 1) Salary: detect_currency, unit_multiplier, _num, parse_salary
# 2) Address: VIETNAM_PROVINCES, CANON, helpers (_norm, _strip_admin_prefix, ...),
#    split_address_all_pairs, split_address_joined, format_pairs
# 3) Job title: _strip_accents, _norm_job_text, SENIORITY_PATTERNS, _job_seniority,
#    GROUP_RULES, _COMPILED_RULES, _job_group, collapse_map
# ===============================================================================================
# ===== Task 1.1 Salary =====
def detect_currency(s: str) -> str:
    s = s.lower()
    if "usd" in s or "$" in s:
        return "USD"
    return "VND"

def unit_multiplier(s: str, currency: str) -> float:
    s = s.lower()
    if currency == "USD":
        return 1000.0 if "k" in s or "nghìn" in s else 1.0
    if "tỷ" in s or "ty" in s: return 1_000_000_000.0
    if "triệu" in s or "trieu" in s or "tr" in s: return 1_000_000.0
    if "nghìn" in s or "nghin" in s or "k" in s: return 1_000.0
    return 1.0

# Extract real numbers from string.
def _num(tok: str):
    tok = tok.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", tok) # integer OR integer.decimal
    return float(m.group(1)) if m else None

# Main function to parse salary string into 4 parts: min/max salary, salary unit and salary note
def parse_salary(text: str):
    # remove null value
    if not isinstance(text, str) or not text.strip():
        return (None, None, "VND", "empty")
    # remove space then lower case
    s = re.sub(r"\s+", " ", text.strip()).lower()

    # case "negotiable"
    if "thoả thuận" in s or "thỏa thuận" in s or "negotiable" in s:
        return (None, None, "VND", "negotiable")

    cur = detect_currency(s)

    # case "range" (10 - 20 tr...)
    m = re.search(r"(\d[\d\.,]*)\s*(?:-|–|—|to|đến|~)\s*(\d[\d\.,]*)\s*(.*)$", s) # [number1] (separator) [number2] (unit)
    if m:
        lo, hi, tail = m.groups()
        mult = unit_multiplier(tail, cur)
        return (_num(lo)*mult, _num(hi)*mult, cur, "range")

    # case "ceiling"
    m = re.search(r"(tới|đến|up to|<=)\s*(\d[\d\.,]*)\s*(.*)$", s)
    if m:
        x, tail = m.group(2), m.group(3)
        return (None, _num(x)*unit_multiplier(tail, cur), cur, "ceiling")

    # case "floor"
    m = re.search(r"(trên|>=|from|từ)\s*(\d[\d\.,]*)\s*(.*)$", s)
    if m:
        x, tail = m.group(2), m.group(3)
        return (_num(x)*unit_multiplier(tail, cur), None, cur, "floor")

    # case specific number
    m = re.search(r"(\d[\d\.,]*)\s*(.*)$", s)
    if m:
        x, tail = m.groups()
        v = _num(x)*unit_multiplier(tail, cur)
        return (v, v, cur, "point")

    # other fallback
    return (None, None, cur, "unparsed")
# ===============================================================================================
# ===== Task 1.2 Address =====
# Canonical list of Vietnam provinces/cities (official names)
VIETNAM_PROVINCES = [
    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu",
    "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước",
    "Bình Thuận", "Cà Mau", "Cần Thơ", "Cao Bằng", "Đà Nẵng",
    "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp",
    "Gia Lai", "Hà Giang", "Hà Nam", "Hà Nội", "Hà Tĩnh",
    "Hải Dương", "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hưng Yên",
    "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng",
    "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An",
    "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình",
    "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng",
    "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa",
    "Thừa Thiên Huế", "Tiền Giang", "TP. Hồ Chí Minh", "Trà Vinh", "Tuyên Quang",
    "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
]

# Normalization map (CANON): variants -> canonical name
CANON = {prov.lower(): prov for prov in VIETNAM_PROVINCES}
CANON.update({
    "hcm": "TP. Hồ Chí Minh", "tp.hcm": "TP. Hồ Chí Minh", "tphcm": "TP. Hồ Chí Minh",
    "sai gon": "TP. Hồ Chí Minh", "sài gòn": "TP. Hồ Chí Minh", "ho chi minh": "TP. Hồ Chí Minh",
    "thanh pho ho chi minh": "TP. Hồ Chí Minh",
    "hn": "Hà Nội", "ha noi": "Hà Nội", "hanoi": "Hà Nội", "thanh pho ha noi": "Hà Nội",
    "da nang": "Đà Nẵng", "thanh pho da nang": "Đà Nẵng",
    "hai phong": "Hải Phòng", "thanh pho hai phong": "Hải Phòng",
    "can tho": "Cần Thơ", "thanh pho can tho": "Cần Thơ",
})

# City lexicon (all recognizable keys)
CITY_LEXICON = set(CANON.keys())

# Utilities
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def _strip_admin_prefix(raw: str) -> str:
    # Remove leading admin words like "TP", "Thành phố", "Tỉnh"
    return re.sub(r"^\s*(tp\.?|thành phố|thanh pho|tỉnh|tinh)\s+", "", raw.strip(), flags=re.I)

def _base_name(raw: str) -> str:
    # Normalized base for comparisons (no admin prefixes)
    return _norm(_strip_admin_prefix(raw))

def _canon_city(raw: str | None) -> str | None:
    if not raw:
        return None
    key = _norm(raw)
    if key in CANON:
        return CANON[key]
    key2 = _base_name(raw)
    if key2 in CANON:
        return CANON[key2]
    # As a last resort, return value without "TP/Thành phố" if present
    no_prefix = _strip_admin_prefix(raw)
    return no_prefix if no_prefix else raw.strip()

def _canon_district(raw: str | None) -> str | None:
    if not raw:
        return None
    # For district: drop "TP/Thành phố/Tỉnh" prefixes, but keep "Quận/Huyện/Thị xã/Phường/Xã"
    return _strip_admin_prefix(raw)

def _is_city(token: str) -> bool:
    t = _norm(token)
    return t in CITY_LEXICON or t.startswith("tp ") or t.startswith("tp.")

def _is_district(token: str) -> bool:
    t = _norm(token)
    if re.search(r"(quận|q\.)\s*\d+", t): return True
    if re.search(r"\bdistrict\b\s*\d*", t): return True
    if re.search(r"\b(huyện|thị xã|phường|xã)\b", t): return True
    return False

# Parse all (city, district) pairs from a cell
def split_address_all_pairs(addr: str):
    if not isinstance(addr, str) or not addr.strip():
        return []
    tokens = [t.strip() for t in re.split(r"[:,;/\-\|;]", addr) if t.strip()]
    if len(tokens) == 1:
        tokens = [t.strip() for t in addr.split(",") if t.strip()]

    pairs = []
    for i in range(0, len(tokens) - 1, 2):
        a, b = tokens[i], tokens[i+1]
        a_city, b_city = _is_city(a), _is_city(b)
        a_dist, b_dist = _is_district(a), _is_district(b)

        # Regular orientations
        if a_city and b_dist:
            city, district = a, b
        elif a_dist and b_city:
            city, district = b, a
        # Both look like cities (e.g., "Hải Dương" vs "TP Hải Dương")
        elif a_city and b_city:
            if _base_name(a) == _base_name(b) or _base_name(a) in _base_name(b) or _base_name(b) in _base_name(a):
                city, district = a, b
            else:
                city, district = a, b
        else:
            city, district = a, b

        city = _canon_city(city)
        district = _canon_district(district)
        pairs.append((city, district))
    return pairs

# Join all pairs back into the same two columns
def split_address_joined(addr: str, max_pairs: int | None = None, dedupe: bool = True):
    pairs = split_address_all_pairs(addr)
    if not pairs:
        return (None, None)
    shown = pairs if max_pairs is None else pairs[:max_pairs]
    cities = [c for c, _ in shown if c]
    dists  = [d for _, d in shown if d]
    if dedupe:
        cities = list(OrderedDict.fromkeys(cities))
        dists  = list(OrderedDict.fromkeys(dists))
    return (", ".join(cities) if cities else None,
            ", ".join(dists) if dists else None)

def format_pairs(addr: str):
    pairs = split_address_all_pairs(addr)
    if not pairs:
        return None
    return " và ".join([", ".join([x for x in pair if x]) for pair in pairs])
# ===============================================================================================
# ===== Task 1.3  job_title =====

# --- Helpers (scoped to job title normalization) ---
def _strip_accents(s: str) -> str:
    # Remove accents to make Vietnamese/English matching robust."""
    if not isinstance(s, str):
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

def _norm_job_text(s: str) -> str:
    # Lowercase, strip accents, unify tech tokens, normalize spaces."""
    s = _strip_accents(str(s)).lower().strip()
    # unify tech tokens commonly seen in titles
    s = (s.replace("node.js", "nodejs")
           .replace("next.js", "nextjs")
           .replace("nuxt.js", "nuxtjs")
           .replace("c/c++", "cpp")
           .replace("c++", "cpp")
           .replace("c#", "csharp")
           .replace(".net", "dotnet"))
    # keep word chars and a few separators; drop the rest
    s = re.sub(r"[^\w\s\+\.#/-]", " ", s)
    s = re.sub(r"[-_/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# --- Seniority extraction (ordered) ---
SENIORITY_PATTERNS = OrderedDict([
    ("intern",     [r"\bintern\b", r"\bthuc tap\b", r"\bthuc tap sinh\b"]),
    ("fresher",    [r"\bfresher\b"]),
    ("junior",     [r"\bjunior\b", r"\bjr\b"]),
    ("mid",        [r"\bmid\b", r"\bmiddle\b", r"\bmid level\b"]),
    ("senior",     [r"\bsenior\b", r"\bsr\b"]),
    ("lead",       [r"\blead\b", r"\btech lead\b", r"\bleader\b"]),
    ("principal",  [r"\bprincipal\b"]),
    ("manager",    [r"\bmanager\b", r"\bengineering manager\b", r"\bquan ly\b"]),
    ("director",   [r"\bdirector\b"]),
    ("head",       [r"\bhead\b", r"\bhead of\b"]),
    ("vp",         [r"\bvice president\b", r"\bvp\b"]),
    ("cto",        [r"\bcto\b", r"\bchief technology officer\b"]),
])

def _job_seniority(title: str) -> str:
    # Return detected seniority level or 'unknown'."""
    txt = _norm_job_text(title)
    for lvl, pats in SENIORITY_PATTERNS.items():
        for p in pats:
            if re.search(p, txt):
                return lvl
    return "unknown"

# --- Fine-grained IT groups (order matters: specific → general) ---
GROUP_RULES = OrderedDict([
    ("fullstack_engineer", [
        r"\bfull\s*stack\b", r"\bfullstack\b", r"\bmern\b", r"\bmean\b", r"\bmevn\b"
    ]),
    ("backend_engineer", [
        r"\bbackend\b", r"\bback\s*end\b", r"\bbe\b", r"\bserver\s*side\b",
        r"\bapi (dev|engineer|developer)\b",
        r"\bjava\b", r"\bspring\b", r"\bdotnet\b", r"\bcsharp\b",
        r"\bpython\b", r"\bdjango\b", r"\bflask\b", r"\bfastapi\b",
        r"\bnodejs\b", r"\bexpress\b", r"\bnestjs\b",
        r"\bgo\b", r"\bgolang\b", r"\brust\b", r"\bscala\b",
        r"\bphp\b", r"\blaravel\b", r"\bsymfony\b",
        r"\bruby\b", r"\brails\b"
    ]),
    ("frontend_engineer", [
        r"\bfrontend\b", r"\bfront\s*end\b", r"\bweb developer\b",
        r"\breact\b", r"\bnextjs\b", r"\bangular\b", r"\bangularjs\b",
        r"\bvue\b", r"\bnuxtjs\b", r"\bsvelte\b",
        r"\bjavascript\b", r"\btypescript\b", r"\bhtml\b", r"\bcss\b"
    ]),
    ("mobile_engineer", [
        r"\bmobile\b", r"\bandroid\b", r"\bios\b", r"\bflutter\b",
        r"\breact native\b", r"\bxamarin\b", r"\bswift\b", r"\bkotlin\b"
    ]),
    ("data_engineer", [
        r"\bdata engineer\b", r"\betl\b", r"\bpipeline\b", r"\bingest(ion)?\b",
        r"\bairflow\b", r"\bspark\b", r"\bhadoop\b", r"\bdatabricks\b",
        r"\bsnowflake\b", r"\bredshift\b", r"\bbig\s*data\b", r"\bglue\b", r"\bdwh\b",
        r"\banalytics engineer\b", r"\bdbt\b"
    ]),
    ("data_science_ml_ai", [
        r"\bdata scientist\b", r"\bml engineer\b", r"\bmachine learning\b",
        r"\bdeep learning\b", r"\bai engineer\b", r"\bnlp\b", r"\bcomputer vision\b",
        r"\bresearch scientist\b", r"\bresearch engineer\b"
    ]),
    ("data_analyst_bi", [
        r"\bdata analyst\b", r"\bbi\b", r"\bbi analyst\b", r"\bpower bi\b",
        r"\btableau\b", r"\blooker\b", r"\bdax\b", r"\breport(ing)?\b",
        r"\bbusiness intelligence\b", r"\banalytics\b",
        r"\bbusiness analyst\b"
    ]),
    ("mlops_dataops", [
        r"\bmlops\b", r"\bdataops\b", r"\bfeature store\b", r"\bmodel serving\b", r"\bmodel ops\b"
    ]),
    ("devops_sre_cloud", [
        r"\bdevops\b", r"\bsre\b", r"\bplatform engineer\b", r"\bcloud engineer\b",
        r"\bkubernetes\b", r"\bk8s\b", r"\bdocker\b", r"\bterraform\b", r"\bhelm\b",
        r"\bci/?cd\b", r"\bgcp\b", r"\baws\b", r"\bazure\b",
        # catch infra/platform variants (fixes cases like "IT Infra Lead")
        r"\b(it\s*)?infra(structure)?\b", r"\binfrastructure\b",
        r"\bplatform( engineer| team| ops)?\b"
    ]),
    ("qa_testing", [
        r"\bqa\b", r"\bquality assurance\b", r"\btester\b", r"\btesting\b",
        r"\bautomation tester\b", r"\bmanual tester\b", r"\bsdet\b", r"\bquality engineer\b"
    ]),
    ("security", [
        r"\bsecurity\b", r"\binfosec\b", r"\bsecops\b", r"\bsoc\b", r"\bpentest(er)?\b",
        r"\bappsec\b", r"\biam\b", r"\bsecurity engineer\b"
    ]),
    ("database_admin", [
        r"\bdba\b", r"\bdatabase administrator\b", r"\bpostgres\b", r"\bmysql\b",
        r"\bsql server\b", r"\boracle\b", r"\bmongodb\b", r"\bredis\b"
    ]),
    ("network_engineer", [
        r"\bnetwork\b", r"\bnetwork engineer\b", r"\bnetwork admin(istrator)?\b", r"\bcisco\b"
    ]),
    ("sysadmin_it_support", [
        r"\bsysadmin\b", r"\bsystem administrator\b", r"\bit admin\b", r"\bit support\b",
        r"\bhelp ?desk\b", r"\bdesktop support\b", r"\bnoc\b",
        r"\bapplication support\b", r"\bit application support\b"
    ]),
    ("architect", [
        r"\bsoftware architect\b", r"\bsolutions? architect\b", r"\benterprise architect\b"
    ]),
    ("product_manager", [
        r"\bproduct manager\b", r"\bpm\b", r"\bproduct owner\b", r"\bpo\b"
    ]),
    ("project_delivery", [
        r"\bproject manager\b", r"\bscrum master\b", r"\bagile coach\b"
    ]),
    ("design_ui_ux", [
        r"\bui/ux\b", r"\bui\b", r"\bux\b", r"\bproduct designer\b", r"\bux researcher\b",
        r"\bgraphic (designer|design)\b"
    ]),
    ("game_dev", [
        r"\bgame developer\b", r"\bunity\b", r"\bunreal\b", r"\bgame programmer\b", r"\bgame designer\b"
    ]),
    ("embedded_firmware_iot", [
        r"\bembedded\b", r"\bfirmware\b", r"\biot\b", r"\brtos\b", r"\bmicrocontroller\b"
    ]),
    ("blockchain_web3", [
        r"\bblockchain\b", r"\bweb3\b", r"\bsolidity\b", r"\bsmart contract\b", r"\bdefi\b"
    ]),
    ("erp_crm", [
        r"\berp\b", r"\bsap\b", r"\bodoo\b", r"\bdynamics\b", r"\bsalesforce\b", r"\bcrm\b"
    ]),
    ("rpa_engineer", [
        r"\brpa\b", r"\buipath\b", r"\bblue prism\b", r"\bautomation anywhere\b"
    ]),
    ("devrel", [
        r"\bdeveloper relations\b", r"\bdevrel\b", r"\bdeveloper advocate\b", r"\bevangelist\b"
    ]),
    ("solutions_sales_engineer", [
        r"\bsolutions? engineer\b", r"\bpre[- ]sales\b", r"\bsolution consultant\b"
    ]),
    ("technical_writer", [
        r"\btechnical writer\b", r"\bdocumentation\b"
    ]),
    # Keep this last as a general catch-all (incl. Vietnamese titles)
    ("software_engineer_general", [
        r"\bsoftware engineer\b", r"\bsoftware developer\b", r"\bdeveloper\b", r"\bprogrammer\b",
        r"\bswe\b", r"\bsde\b", r"\bengineer\b",
        r"\blap trinh vien\b", r"\blap trinh\b",
        r"\bky su phan mem\b", r"\bchuyen vien lap trinh\b"
    ]),
])

# Pre-compile once for performance
_COMPILED_RULES = [(grp, [re.compile(p) for p in pats]) for grp, pats in GROUP_RULES.items()]

def _job_group(title: str) -> str:
    # Return the best-matching fine-grained group, else 'other_it'."""
    txt = _norm_job_text(title)
    for grp, patterns in _COMPILED_RULES:
        for p in patterns:
            if p.search(txt):
                return grp
    return "other_it"

# --- Collapse to 8 big buckets ---
collapse_map = {
    # 1) Software Engineering (programming)
    "software_engineer_general": "software_engineering",
    "backend_engineer":          "software_engineering",
    "frontend_engineer":         "software_engineering",
    "fullstack_engineer":        "software_engineering",
    "mobile_engineer":           "software_engineering",
    "game_dev":                  "software_engineering",
    "embedded_firmware_iot":     "software_engineering",
    "blockchain_web3":           "software_engineering",
    "erp_crm":                   "software_engineering",
    "rpa_engineer":              "software_engineering",
    "architect":                 "software_engineering",

    # 2) Data
    "data_engineer":             "data",
    "data_analyst_bi":           "data",
    "data_science_ml_ai":        "data",
    "mlops_dataops":             "data",

    # 3) Infra/Cloud (DevOps/SRE/Platform)
    "devops_sre_cloud":          "infra_cloud",

    # 4) QA/Testing
    "qa_testing":                "qa_testing",

    # 5) Security
    "security":                  "security",

    # 6) IT Ops/Support
    "sysadmin_it_support":       "it_ops_support",
    "database_admin":            "it_ops_support",
    "network_engineer":          "it_ops_support",

    # 7) Product/Project
    "product_manager":           "product_project",
    "project_delivery":          "product_project",

    # 8) Design (UI/UX)
    "design_ui_ux":              "design",

    # Others / edge roles
    "devrel":                    "other",
    "solutions_sales_engineer":  "other",
    "technical_writer":          "other",
    "other_it":                  "other",
}

#=========================== Check schema, quarantine error records ======================================
def _validate_schema(df: pd.DataFrame):
    if not STRICT_SCHEMA:
        return
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def _quarantine_df(df: pd.DataFrame, reason: str, run_id: str, chunk_idx: int):
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    path = os.path.join(QUARANTINE_DIR, f"bad_rows_{run_id}_chunk{chunk_idx}_{ts}.csv")
    df.to_csv(path, index=False)
    logger.error(f"Quarantined {len(df)} rows to {path} | reason={reason}")

def transform(df: pd.DataFrame, run_id: str = "na", chunk_idx: int = 0) -> pd.DataFrame:
    #Apply all Task 1 transforms. Any unexpected exception quarantines the whole chunk.
    _validate_schema(df)
    out = df.copy()

    try:
        # Salary
        if "salary" in out.columns:
            parsed = out["salary"].apply(parse_salary)
            out["min_salary"], out["max_salary"], out["salary_unit"], out["salary_note"] = zip(*parsed)

        # Address
        if "address" in out.columns:
            out["city"], out["district"] = zip(*out["address"].apply(lambda s: split_address_joined(s, max_pairs=None, dedupe=True)))
            out["city_district_pairs_str"] = out["address"].apply(format_pairs)

        # Job title
        if "job_title" in out.columns:
            out["job_title_group"] = out["job_title"].apply(_job_group)
            out["job_seniority"]   = out["job_title"].apply(_job_seniority)
            out["job_title_group_big"] = out["job_title_group"].map(collapse_map).fillna("other")

        # Type coercion (reduce error for load)
        for col in ("min_salary", "max_salary"):
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")

        return out

    except Exception as e:
        logger.error(f"Transform error on chunk {chunk_idx}: {e}")
        logger.debug(traceback.format_exc())
        _quarantine_df(out, reason=f"transform_error:{e}", run_id=run_id, chunk_idx=chunk_idx)
        # propagate let pipeline decide if need for skipping chunk
        raise