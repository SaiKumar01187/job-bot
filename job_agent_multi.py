
import os, re, csv, json, hashlib, requests, pandas as pd
from urllib.parse import urlparse
from datetime import datetime, timezone

INPUT_EXCEL = os.getenv("INPUT_EXCEL", "companies.xlsx")
OUTPUT_DIR  = os.getenv("OUTPUT_DIR", "out")
SEEN_PATH   = os.getenv("SEEN_PATH", "seen.csv")
TIMEOUT     = int(os.getenv("HTTP_TIMEOUT", "20"))
UA          = os.getenv("HTTP_UA", "job-agent-multi/1.0")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def sha1(s): return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def load_seen(path):
    s=set()
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if row: s.add(row[0])
    return s

def save_seen(path, keys):
    with open(path, "a", newline="", encoding="utf-8") as f:
        w=csv.writer(f); [w.writerow([k]) for k in keys]

def strip_html(t): return re.sub(r"<[^>]+>", " ", t or "").strip()

def get_json(url, params=None, method="GET", body=None):
    headers={"User-Agent": UA, "Accept": "application/json"}
    if method == "POST":
        headers["Content-Type"] = "application/json"
        r = requests.post(url, params=params, data=json.dumps(body or {}), headers=headers, timeout=TIMEOUT)
    else:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# ---------------- Providers -----------------

def fetch_greenhouse(slug, name):
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    out = []
    try:
        data = get_json(url, {"content":"true"})
        for j in data.get("jobs", []):
            out.append({
                "company": name or slug,
                "title": j.get("title"),
                "location": (j.get("location") or {}).get("name",""),
                "url": j.get("absolute_url"),
                "source": "Greenhouse",
                "postedAt": j.get("updated_at"),
                "snippet": strip_html(j.get("content",""))[:280],
            })
    except Exception as e:
        print("[warn] greenhouse", slug, e)
    return out

def fetch_lever(slug, name):
    url = f"https://api.lever.co/v0/postings/{slug}"
    out = []
    try:
        data = get_json(url, {"mode":"json"})
        for j in data:
            if j.get("state","published").lower() != "published": continue
            cat=j.get("categories") or {}
            out.append({
                "company": name or slug,
                "title": j.get("text"),
                "location": cat.get("location") or cat.get("team") or "",
                "url": j.get("hostedUrl"),
                "source": "Lever",
                "postedAt": (datetime.fromtimestamp(j.get("createdAt",0)/1000, tz=timezone.utc).isoformat()
                             if j.get("createdAt") else None),
                "snippet": (j.get("descriptionPlain") or "")[:280],
            })
    except Exception as e:
        print("[warn] lever", slug, e)
    return out

def fetch_workable(slug, name):
    # Public job board JSON
    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    out = []
    try:
        data = get_json(url, {"active":"true"})
        for j in data.get("results", []):
            out.append({
                "company": name or slug,
                "title": j.get("title"),
                "location": (j.get("location",{}) or {}).get("city",""),
                "url": j.get("application_url") or j.get("url"),
                "source": "Workable",
                "postedAt": j.get("published_on") or j.get("updated_at"),
                "snippet": strip_html(j.get("shortcode",""))[:280],
            })
    except Exception as e:
        print("[warn] workable", slug, e)
    return out

def fetch_smartrecruiters(slug, name):
    # Company slug often equals the subdomain on smartrecruiters.com; may vary
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    out = []
    try:
        data = get_json(url, {"limit":"100"})
        for j in data.get("content", []):
            # Build posting URL if not present
            u = j.get("ref",{}).get("jobAdUrl") or j.get("applyUrl") or j.get("postingUrl")
            if not u and j.get("id"):
                u = f"https://jobs.smartrecruiters.com/{slug}/{j['id']}"
            out.append({
                "company": name or slug,
                "title": j.get("name"),
                "location": (j.get("location",{}) or {}).get("city",""),
                "url": u,
                "source": "SmartRecruiters",
                "postedAt": j.get("releasedDate") or j.get("createdOn"),
                "snippet": strip_html(j.get("jobAd",{}).get("sections",{}).get("companyDescription",""))[:280],
            })
    except Exception as e:
        print("[warn] smartrecruiters", slug, e)
    return out

def fetch_ashby(slug, name):
    # Ashby needs a small GraphQL POST to their public endpoint
    url = "https://jobs.ashbyhq.com/api/non-user-graphql"
    query = {
        "operationName":"JobBoardAllPositions",
        "variables":{"organizationSlug": slug},
        "query":"query JobBoardAllPositions($organizationSlug: String!) { jobBoard: jobBoardWithEmail(organizationSlug: $organizationSlug) { teams { name jobs { id title locationSlug locationName applyUrl publishedAt } } } }"
    }
    out = []
    try:
        data = get_json(url, method="POST", body=query)
        board = (data or {}).get("data",{}).get("jobBoard",{}) or {}
        for team in board.get("teams",[]) or []:
            for j in team.get("jobs",[]) or []:
                out.append({
                    "company": name or slug,
                    "title": j.get("title"),
                    "location": j.get("locationName") or j.get("locationSlug") or "",
                    "url": j.get("applyUrl"),
                    "source": "Ashby",
                    "postedAt": j.get("publishedAt"),
                    "snippet": "",
                })
    except Exception as e:
        print("[warn] ashby", slug, e)
    return out

def fetch_workday(career_url, name):
    # Best-effort: derive cxs endpoint from a typical Workday URL like:
    # https://<tenant>.myworkdayjobs.com/<careerSite>/...
    # Then call: https://<tenant>.myworkdayjobs.com/wday/cxs/<tenant>/<careerSite>/jobs
    out = []
    try:
        u = urlparse(career_url)
        host = u.netloc
        parts = [p for p in u.path.split('/') if p]
        if ".myworkdayjobs.com" not in host or len(parts) < 1:
            return out
        tenant = host.split(".")[0]
        site = parts[0]
        api = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
        data = get_json(api, method="POST", body={"appliedFacets":{}, "limit": 50, "offset": 0, "searchText": ""})
        for j in data.get("jobPostings",[]) or []:
            out.append({
                "company": name or tenant,
                "title": j.get("title"),
                "location": (j.get("locations",[]) or [None])[0] or "",
                "url": f"https://{host}{j.get('externalPath','')}",
                "source": "Workday",
                "postedAt": j.get("postedOn") or j.get("startDate"),
                "snippet": strip_html(j.get("shortDescription",""))[:280],
            })
    except Exception as e:
        print("[warn] workday", career_url, e)
    return out

# -------------- Detection -------------------
def detect_provider(provider, slug, career_url):
    prov = (provider or "").strip().lower()
    if prov in {"greenhouse","lever","workable","ashby","smartrecruiters","workday"}:
        return prov
    host = urlparse(career_url or "").netloc.lower()
    if "greenhouse.io" in host: return "greenhouse"
    if "lever.co" in host: return "lever"
    if "workable.com" in host: return "workable"
    if "ashbyhq.com" in host or "jobs.ashbyhq.com" in host: return "ashby"
    if "smartrecruiters.com" in host: return "smartrecruiters"
    if "myworkdayjobs.com" in host: return "workday"
    return ""

def slug_from_url(prov, career_url):
    path = urlparse(career_url or "").path.strip("/")
    parts = path.split("/") if path else []
    if prov == "greenhouse":
        # boards.greenhouse.io/<slug>
        return parts[0] if parts else ""
    if prov == "lever":
        # jobs.lever.co/<slug>
        return parts[0] if parts else ""
    if prov == "workable":
        # apply.workable.com/<slug>/
        return parts[0] if parts else ""
    if prov == "ashby":
        # jobs.ashbyhq.com/<slug>
        return parts[0] if parts else ""
    if prov == "smartrecruiters":
        # jobs.smartrecruiters.com/<slug>/...
        return parts[0] if parts else ""
    return ""

def keyword_filter(rows, kw_str):
    kws = [k.strip().lower() for k in str(kw_str or "").split(";") if k.strip()]
    if not kws: return rows
    out = []
    for r in rows:
        hay = f"{r.get('title','')} {r.get('snippet','')} {r.get('location','')}".lower()
        if any(k in hay for k in kws):
            out.append(r)
    return out

def main():
    df = pd.read_excel(INPUT_EXCEL).fillna("")
    all_rows = []
    for _, row in df.iterrows():
        name = str(row.get("company_name","")).strip()
        provider = str(row.get("provider","")).strip().lower()
        slug = str(row.get("slug","")).strip()
        career_url = str(row.get("career_url","")).strip()
        keywords = str(row.get("keywords",""))

        prov = detect_provider(provider, slug, career_url)

        if prov in ("greenhouse","lever","workable","ashby","smartrecruiters"):
            if not slug:
                slug = slug_from_url(prov, career_url)
            if not slug and prov != "workday":
                print(f"[info] No slug for {name} ({prov}); skipping")
                rows = []
            else:
                rows = (
                    fetch_greenhouse(slug, name) if prov=="greenhouse" else
                    fetch_lever(slug, name) if prov=="lever" else
                    fetch_workable(slug, name) if prov=="workable" else
                    fetch_ashby(slug, name) if prov=="ashby" else
                    fetch_smartrecruiters(slug, name) if prov=="smartrecruiters" else
                    []
                )
        elif prov == "workday":
            rows = fetch_workday(career_url, name)
        else:
            print(f"[info] Unknown provider for {name}; provide career_url or provider/slug")
            rows = []

        rows = keyword_filter(rows, keywords)
        all_rows.extend(rows)

    # Dedup by URL hash
    seen = load_seen(SEEN_PATH)
    fresh=[]; newkeys=set()
    for r in all_rows:
        key = sha1(r.get("url"))
        if key not in seen:
            fresh.append(r); newkeys.add(key)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUTPUT_DIR, f"new_openings_{ts}.xlsx")
    if fresh:
        pd.DataFrame(fresh).to_excel(out_path, index=False)
        save_seen(SEEN_PATH, newkeys)
        print("wrote", len(fresh), "new rows ->", out_path)
    else:
        pd.DataFrame(columns=["company","title","location","url","source","postedAt","snippet"]).to_excel(out_path, index=False)
        print("no new rows ->", out_path)

if __name__ == "__main__":
    main()
