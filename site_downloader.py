#!/usr/bin/env python3
"""
site_downloader.py
------------------
Downloads all files available as hyperlinks on a given public webpage.

Usage:
    python site_downloader.py <url> [options]

Options:
    -o, --output-dir    Directory to save files (default: ./downloads)
    -e, --extensions    Comma-separated list of extensions to filter (e.g. pdf,xlsx,csv)
                        If omitted, all linked files are downloaded.
    -d, --delay         Delay in seconds between requests (default: 0.5)
    -r, --recursive     Follow links to same-domain pages and download from those too
    --depth             Max recursion depth when -r is used (default: 1)
    --dry-run           Print what would be downloaded without actually downloading
    --no-verify-ssl     Disable SSL certificate verification
    -v, --verbose       Verbose logging

Examples:
    python site_downloader.py https://example.com/resources
    python site_downloader.py https://example.com/data -e pdf,xlsx,csv -o ./my_files
    python site_downloader.py https://example.com -r --depth 2 -e pdf
"""

import argparse
import logging
import mimetypes
import os
import re
import sys
import time
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:\n  pip install requests beautifulsoup4")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("site_downloader")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FILE_LIKE_EXTENSIONS = {
    # Documents
    "pdf", "doc", "docx", "xls", "xlsx", "xlsm", "ppt", "pptx", "odt", "ods", "odp",
    # Data
    "csv", "tsv", "json", "xml", "yaml", "yml", "sql",
    # Archives
    "zip", "tar", "gz", "bz2", "7z", "rar", "tgz",
    # Text / code
    "txt", "md", "rst", "log", "py", "js", "ts", "cs", "java", "sh", "bat", "ps1",
    # Media
    "jpg", "jpeg", "png", "gif", "svg", "mp3", "mp4", "wav", "avi", "mov",
    # Misc
    "exe", "msi", "dmg", "iso", "deb", "rpm",
}


def normalize_url(url: str) -> str:
    """Strip query string and fragment for dedup purposes."""
    parsed = urlparse(url)
    return parsed._replace(query="", fragment="").geturl()


def is_file_link(href: str, allowed_exts: set | None) -> bool:
    """Return True if the href points to a downloadable file."""
    path = urlparse(href).path
    ext = Path(unquote(path)).suffix.lstrip(".").lower()
    if not ext:
        return False
    if allowed_exts:
        return ext in allowed_exts
    return ext in FILE_LIKE_EXTENSIONS


def safe_filename(url: str, output_dir: Path, existing: set) -> Path:
    """
    Derive a filesystem-safe filename from the URL.
    Appends a counter suffix if a name collision exists.
    """
    path = unquote(urlparse(url).path)
    name = os.path.basename(path) or "download"
    # Sanitize
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    stem, _, suffix = name.rpartition(".")
    suffix = f".{suffix}" if suffix else ""

    candidate = output_dir / name
    counter = 1
    while str(candidate) in existing or candidate.exists():
        candidate = output_dir / f"{stem}_{counter}{suffix}"
        counter += 1

    existing.add(str(candidate))
    return candidate


def get_session(verify_ssl: bool) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; SiteFileDownloader/1.0; "
            "+https://github.com/your-org/site-downloader)"
        )
    })
    session.verify = verify_ssl
    return session


def fetch_page(session: requests.Session, url: str) -> BeautifulSoup | None:
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as exc:
        log.warning("Could not fetch page %s: %s", url, exc)
        return None


def download_file(
    session: requests.Session,
    url: str,
    dest: Path,
    dry_run: bool,
) -> bool:
    if dry_run:
        log.info("[DRY-RUN] Would download: %s → %s", url, dest)
        return True

    try:
        with session.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", 0))
            dest.parent.mkdir(parents=True, exist_ok=True)
            downloaded = 0
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
                        downloaded += len(chunk)

        size_str = f"{downloaded / 1024:.1f} KB" if downloaded < 1_048_576 else f"{downloaded / 1_048_576:.2f} MB"
        log.info("✓  %-60s  %s", dest.name, size_str)
        return True

    except requests.RequestException as exc:
        log.error("✗  Failed %s: %s", url, exc)
        return False


def collect_links(
    soup: BeautifulSoup,
    base_url: str,
    allowed_exts: set | None,
    same_domain_only: bool,
) -> tuple[list[str], list[str]]:
    """
    Returns (file_links, page_links) both as absolute URLs.
    """
    base_domain = urlparse(base_url).netloc
    file_links = []
    page_links = []

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue

        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)

        if parsed.scheme not in ("http", "https"):
            continue

        if same_domain_only and parsed.netloc != base_domain:
            continue

        if is_file_link(abs_url, allowed_exts):
            file_links.append(abs_url)
        else:
            # Candidate for recursive crawl
            page_links.append(abs_url)

    return file_links, page_links


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    allowed_exts = None
    if args.extensions:
        allowed_exts = {e.strip().lower().lstrip(".") for e in args.extensions.split(",")}
        log.info("Filtering for extensions: %s", ", ".join(sorted(allowed_exts)))

    session = get_session(verify_ssl=not args.no_verify_ssl)

    visited_pages: set[str] = set()
    queued_files: set[str] = set()
    used_filenames: set[str] = set()

    # BFS queue: (url, depth)
    page_queue: deque[tuple[str, int]] = deque()
    page_queue.append((args.url, 0))

    downloaded = 0
    skipped = 0
    failed = 0

    while page_queue:
        page_url, depth = page_queue.popleft()
        norm = normalize_url(page_url)
        if norm in visited_pages:
            continue
        visited_pages.add(norm)

        log.info("Scanning page [depth=%d]: %s", depth, page_url)
        soup = fetch_page(session, page_url)
        if soup is None:
            continue

        file_links, page_links = collect_links(
            soup, page_url, allowed_exts, same_domain_only=True
        )

        # Enqueue file downloads
        for furl in file_links:
            norm_f = normalize_url(furl)
            if norm_f in queued_files:
                log.debug("Already queued: %s", furl)
                skipped += 1
                continue
            queued_files.add(norm_f)

            dest = safe_filename(furl, output_dir, used_filenames)
            ok = download_file(session, furl, dest, dry_run=args.dry_run)
            if ok:
                downloaded += 1
            else:
                failed += 1

            if args.delay > 0 and not args.dry_run:
                time.sleep(args.delay)

        # Enqueue child pages if recursive
        if args.recursive and depth < args.depth:
            for purl in page_links:
                norm_p = normalize_url(purl)
                if norm_p not in visited_pages:
                    page_queue.append((purl, depth + 1))

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print("\n" + "─" * 60)
    print(f"  Pages scanned   : {len(visited_pages)}")
    print(f"  Files downloaded: {downloaded}")
    print(f"  Duplicates skip : {skipped}")
    print(f"  Failures        : {failed}")
    print(f"  Output dir      : {output_dir.resolve()}")
    print("─" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download all linked files from a public website.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("url", help="Starting URL to scrape")
    p.add_argument("-o", "--output-dir", default="./downloads", metavar="DIR",
                   help="Directory to save downloaded files (default: ./downloads)")
    p.add_argument("-e", "--extensions", default=None, metavar="EXT,...",
                   help="Comma-separated extensions to download (e.g. pdf,xlsx). "
                        "Omit to download all recognised file types.")
    p.add_argument("-d", "--delay", type=float, default=0.5, metavar="SEC",
                   help="Seconds to wait between downloads (default: 0.5)")
    p.add_argument("-r", "--recursive", action="store_true",
                   help="Follow same-domain page links recursively")
    p.add_argument("--depth", type=int, default=1, metavar="N",
                   help="Max recursion depth when --recursive is set (default: 1)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be downloaded without saving files")
    p.add_argument("--no-verify-ssl", action="store_true",
                   help="Disable SSL certificate verification")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Enable debug logging")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info("Starting download from: %s", args.url)
    run(args)


if __name__ == "__main__":
    main()
