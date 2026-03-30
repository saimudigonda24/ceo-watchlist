# api/app/scripts/scrape_ceo_changes.py
import re
import json
import sys
import argparse
from typing import List, Dict

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from pathlib import Path, PurePosixPath
import json


URL = "https://intellizence.com/insights/executive-appointments/latest-ceo-changes-and-appointments/"
UA = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# Simple patterns we care about
SENTENCE_PAT = re.compile(
    r"""(?ix)
    \b(
        appointed|appoints|names?|named|promotes?|hired?|hires|
        will\s+become|to\s+be\s+named|stepp?ing\s+down|retire[s|d]?|
        interim\s+CEO|CEO\s+designate
    )\b
    .*?\b(CEO|Chief\s+Executive\s+Officer)\b
    """,
)

# Lightweight person/company extraction heuristics
def extract_person_company(sentence: str) -> (str, str): # type: ignore
    # Try “<Company> has appointed <Person> as (new) CEO …”
    m = re.search(r"(?i)(.*?)\b(has\s+appointed|appointed|has\s+named|named|names?)\s+([^,]+?)\s+(as\s+)?(new\s+)?(global\s+)?(interim\s+)?(chief\s+executive\s+officer|CEO)\b", sentence)
    if m:
        company = m.group(1).strip(" ,–—-")
        person = m.group(3).strip(" ,–—-")
        return (person, company)

    # “<Person> to be named CEO of <Company>”
    m = re.search(r"(?i)([^,]+?)\s+(to\s+be\s+named|will\s+become)\s+(?:the\s+)?(new\s+)?(global\s+)?(interim\s+)?(CEO|chief\s+executive\s+officer)\s+(of|at)\s+([^.,;]+)", sentence)
    if m:
        person = m.group(1).strip(" ,–—-")
        company = m.group(8).strip(" ,–—-")
        return (person, company)

    # “<Company> names <Person> CEO”
    m = re.search(r"(?i)(.*?)\s+names?\s+([^,]+?)\s+(?:as\s+)?(new\s+)?(global\s+)?(interim\s+)?(CEO|chief\s+executive\s+officer)\b", sentence)
    if m:
        company = m.group(1).strip(" ,–—-")
        person = m.group(2).strip(" ,–—-")
        return (person, company)

    # “<Person> appointed CEO, <Company>”
    m = re.search(r"(?i)([^,]+?)\s+(?:is\s+)?appointed\s+(?:as\s+)?(new\s+)?(global\s+)?(interim\s+)?(CEO|chief\s+executive\s+officer)\b.*?(?:of|at)?\s*([^.,;]+)?", sentence)
    if m:
        person = m.group(1).strip(" ,–—-")
        company = (m.group(5) or "").strip(" ,–—-")
        return (person, company)

    # Fallback: guess last organization-like phrase after “of/at”
    m = re.search(r"(?i)\b(CEO|chief\s+executive\s+officer)\b.*?(?:of|at)\s+([^.,;]+)", sentence)
    person_guess = ""
    if m:
        company = m.group(2).strip(" ,–—-")
    else:
        company = ""
    # Person guess: capitalized word sequence before “appointed/named/…”
    m2 = re.search(r"(?i)([A-Z][\w\.'\-]+(?:\s+[A-Z][\w\.'\-]+){0,3})\s+(appointed|named|to\s+be\s+named|will\s+become)", sentence)
    if m2:
        person_guess = m2.group(1).strip()
    return (person_guess, company)

def visible_text(el: Tag) -> str:
    texts: List[str] = []
    for node in el.descendants:
        if isinstance(node, NavigableString):
            t = str(node).strip()
            if t:
                texts.append(t)
    return " ".join(texts)

def parse_blocks(soup: BeautifulSoup) -> List[str]:
    candidates: List[str] = []
    # Try obvious article content regions
    for sel in [
        "article", ".post-content", ".entry-content", ".blog-post", ".td-post-content",
        "main", ".content", ".elementor-widget-container"
    ]:
        for block in soup.select(sel):
            txt = visible_text(block)
            if txt and len(txt) > 200:
                candidates.append(txt)

    # Also collect list items and paragraphs which often contain single announcements
    for el in soup.select("li, p"):
        t = el.get_text(" ", strip=True)
        if t and len(t) > 40:
            candidates.append(t)

    return candidates

def split_sentences(text: str) -> List[str]:
    # Conservative sentence splitter
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-Z0-9])", text)
    return [p.strip() for p in parts if p and len(p) > 30]

def scrape(debug_dump: bool=False) -> List[Dict]:
    r = requests.get(URL, headers=UA, timeout=30)
    r.raise_for_status()
    html = r.text

    if debug_dump:
        with open("/tmp/intellizence_ceo_changes.html", "w", encoding="utf-8") as f:
            f.write(html)

    soup = BeautifulSoup(html, "lxml")
    blocks = parse_blocks(soup)

    items = []
    seen = set()

    for block in blocks:
        for sent in split_sentences(block):
            if SENTENCE_PAT.search(sent):
                key = sent.lower()
                if key in seen:
                    continue
                seen.add(key)
                person, company = extract_person_company(sent)
                items.append({
                    "source": URL,
                    "person": person,
                    "company": company,
                    "role": "CEO",
                    "headline": sent.rstrip("."),
                })

    return items

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", action="store_true", help="Save fetched HTML to /tmp for debugging")
    args = ap.parse_args()

    try:
        data = scrape(debug_dump=args.dump)
    except Exception as e:
        # Make failures visible to the pipeline callers
        print(json.dumps({"error": str(e)}))
        sys.exit(2)

    print(json.dumps(data, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()

BRONZE = Path("/data/bronze")
BRONZE.mkdir(parents=True, exist_ok=True)
( BRONZE / "intellizence_ceo_changes.json" ).write_text(json.dumps(rows, ensure_ascii=False, indent=2))
print(json.dumps(rows, ensure_ascii=False, indent=2))