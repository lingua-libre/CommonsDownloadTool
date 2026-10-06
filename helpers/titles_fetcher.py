#!/usr/bin/env python3
import contextlib
import gzip
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
import argparse
import sys
import os

VERSION = "3.2.0"

DEFAULT_PATTERN = r".*"
DEFAULT_OUTPUT = "./tmp/titles.txt"
REPLICA_CNF = os.path.expanduser("~/replica.my.cnf")
REPLICA_HOST = "commonswiki.analytics.db.svc.wikimedia.cloud"
REPLICA_DB = "commonswiki_p"
REPLICA_BATCH = 100000

MANUAL = """
titles_fetcher.py — Manual

Description:
    List page titles of a Wikimedia Commons namespace (default 0, main; 6 = files),
    optionally only those starting with a prefix or matching a regex, from either
    the live replica database or the monthly dump.

Synopsis:
    python3 helpers/titles_fetcher.py [OPTIONS]

Options:
    --namespace, -n Namespace number to list (default: 6, File). 
                    See Help:Namespaces on your wiki.
    --prefix, -x    Title prefix to keep, underscore form, no namespace
                    prefix, for example "LL-Q". Rows are pre-filtered on it (fast),
                    so --pattern can only narrow that set. Default: no prefix.
    --source, -s    auto (default), replica, titles or dump.
                    replica: query the Commons replica (Toolforge/PAWS, needs
                             ~/replica.my.cnf and pymysql). Fast, current. Run daily.
                    titles:  stream commonswiki-<date>-all-titles.gz (~1.5 GB).
                             Redirects are NOT excluded (the dump has no flag);
                             commons_download_tool.py skips them at download time.
                    dump:    stream commonswiki-<date>-page.sql.gz (~7 GB), redirects
                             excluded. Run monthly.
                    auto:    replica if reachable, else titles.
    --pattern, -p   Regex a title (underscore form, no "File:" prefix) must match
                    (optional, default: .* = all), for example '^LL-Q\\d{3,9}[_ ].+\\.(?:wav|ogg)$'
    --output, -o    Output file path (default: ./tmp/titles.txt)
    --redirects-output, -r
                    Also save the titles of matching redirect pages to this file.
                    Redirects are always excluded from --output.
    --dump-dir, -d  Keep the downloaded dump in this directory (e.g. ./dumps). A dump
                    already there is reused instead of downloaded again. Without
                    this option the dump is only streamed, not saved.
    --months-back, -m   Number of previous months to try for the dump (default: 2)
    --dry-run       Show what would be done without writing files
    --verbose, -v   Enable verbose output
    --version       Show version and exit
    --help, -h      Show this manual and exit

Examples:
    python3 helpers/titles_fetcher.py       # download pages titles, keeps filenames.
    python3 helpers/titles_fetcher.py -x LL-Q -p '^LL-Q\\d{3,9}[_ ].+\\.(?:wav|ogg)$' -o titles.txt

Notes:
    - Output is one plain filename per line (underscores, no namespace prefix).
    - Files are written to <output>.tmp then renamed, so a failed run leaves the
      previous output intact. Titles are streamed to disk, not held in memory.
    - Exit codes: 0 success, 1 error
"""

headers = {
    "User-Agent": "CommonsDownloadTool/1.0 (https://github.com/lingua-libre/CommonsDownloadTool)"
}

def page_row_regex(namespace, prefix):
    """Regex for a page.sql row: (page_id,<ns>,'<prefix>...',[restrictions,]is_redirect,...)"""
    return re.compile(
        rb"\((\d+)," + str(namespace).encode() + rb",'(" + re.escape(prefix.encode("utf-8")) + rb"(?:[^'\\]|\\.)*)',(?:'[^']*',)?([01]),"
    )


SQL_UNESCAPE = re.compile(rb"\\(.)")


class Tee:
    """File-like reader that also writes everything it reads to a file."""

    def __init__(self, source, out):
        self.source, self.out = source, out

    def read(self, size=-1):
        data = self.source.read(size)
        self.out.write(data)
        return data


def local_dump(url, dump_dir):
    """Path where a dump URL is kept inside dump_dir."""
    return os.path.join(dump_dir, url.rsplit("/", 1)[1])


@contextlib.contextmanager
def open_dump(url, dump_dir=None):
    """Context manager giving a binary stream of the dump; with dump_dir, reuse or save a local copy."""
    if not dump_dir:
        yield urllib.request.urlopen(urllib.request.Request(url, headers=headers))
        return
    path = local_dump(url, dump_dir)
    if os.path.exists(path):
        print(f"Reading local copy {path}")
        with open(path, "rb") as f:
            yield f
        return
    os.makedirs(dump_dir, exist_ok=True)
    done = False
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers)) as response, \
                open(path + ".part", "wb") as out:
            tee = Tee(response, out)
            yield tee
            while tee.read(1 << 20):  # drain what the parser did not read
                pass
            done = True
    finally:
        if done:
            os.replace(path + ".part", path)
            print(f"Saved dump to {path}")
        elif os.path.exists(path + ".part"):
            os.remove(path + ".part")


def iter_dump(url, namespace, prefix, verbose=False, dump_dir=None):
    """Yield (title, is_redirect) for pages of a namespace starting with prefix, from a page.sql.gz stream."""
    page_row = page_row_regex(namespace, prefix)
    seen = 0
    with open_dump(url, dump_dir) as response, gzip.GzipFile(fileobj=response) as gfile:
        for line in gfile:
            if not line.startswith(b"INSERT INTO"):
                continue
            for m in page_row.finditer(line):
                raw = m.group(2)
                if b"\\" in raw:
                    raw = SQL_UNESCAPE.sub(rb"\1", raw)
                yield raw.decode("utf-8", "replace"), m.group(3) == b"1"
                seen += 1
                if verbose and seen % 100000 == 0:
                    print(f"  Scanned {seen} matching pages so far...")


def iter_titles(url, namespace, prefix, verbose=False, dump_dir=None):
    """Yield (title, False) for pages of a namespace starting with prefix, from an all-titles.gz stream ("ns<TAB>title" lines)."""
    start = f"{namespace}\t{prefix}".encode("utf-8")
    seen = 0
    with open_dump(url, dump_dir) as response, gzip.GzipFile(fileobj=response) as gfile:
        for line in gfile:
            if not line.startswith(start):
                continue
            yield line[2:].rstrip(b"\n").decode("utf-8", "replace"), False
            seen += 1
            if verbose and seen % 100000 == 0:
                print(f"  Scanned {seen} matching pages so far...")


def open_replica():
    """Return a pymysql connection to the Commons replica, or None if unavailable."""
    if not os.path.exists(REPLICA_CNF):
        return None
    try:
        import pymysql
        return pymysql.connect(
            host=REPLICA_HOST, database=REPLICA_DB,
            read_default_file=REPLICA_CNF, charset="utf8mb4", connect_timeout=10,
        )
    except Exception as e:
        print(f"Replica unavailable: {e}")
        return None


def iter_replica(conn, namespace, prefix, verbose=False):
    """Yield (title, is_redirect) for pages of a namespace starting with prefix, keyset-paginated on page_id."""
    like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    last_id = 0
    seen = 0
    sql = (
        "SELECT page_id, page_title, page_is_redirect FROM page "
        "WHERE page_namespace = %s AND page_title LIKE %s AND page_id > %s "
        "ORDER BY page_id LIMIT %s"
    )
    with conn.cursor() as cur:
        while True:
            cur.execute(sql, (namespace, like, last_id, REPLICA_BATCH))
            rows = cur.fetchall()
            if not rows:
                return
            for _, title, is_redirect in rows:
                if isinstance(title, (bytes, bytearray)):
                    title = title.decode("utf-8", "replace")
                yield title, bool(is_redirect)
            last_id = rows[-1][0]
            seen += len(rows)
            if verbose:
                print(f"  Scanned {seen} matching pages so far...")


def find_dump(target_urls):
    """Return the first dump URL that exists, or None."""
    for url in target_urls:
        print(f"Trying URL: {url}")
        req = urllib.request.Request(url, headers=headers, method="HEAD")
        try:
            urllib.request.urlopen(req).close()
            print("-> Found.")
            return url
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print("-> Not found (404).")
                continue
            print(f"HTTP Error encountered: {e.code}")
            return None
    return None


def main():
    parser = argparse.ArgumentParser(description="Fetch and filter Commons titles", add_help=False)
    parser.add_argument("--source", "-s", choices=["auto", "replica", "titles", "dump"], default="auto")
    parser.add_argument("--namespace", "-n", type=int, default=0)
    parser.add_argument("--prefix", "-x", default="")
    parser.add_argument("--pattern", "-p", default=DEFAULT_PATTERN)
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT)
    parser.add_argument("--redirects-output", "-r", default=None)
    parser.add_argument("--dump-dir", "-d", default=None)
    parser.add_argument("--months-back", "-m", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--help", "-h", action="store_true")
    args = parser.parse_args()

    if args.version:
        print(f"titles_fetcher.py version {VERSION}")
        return 0

    if args.help:
        print(MANUAL)
        return 0

    pattern = re.compile(args.pattern, re.IGNORECASE)

    if args.verbose:
        print(f"Source: {args.source}")
        print(f"Namespace: {args.namespace}")
        print(f"Prefix: {args.prefix!r}")
        print(f"Pattern: {args.pattern}")
        print(f"Output: {args.output}")

    # Dump URLs, newest month first; "auto" falls back to all-titles when no replica
    dump_kind = "dump" if args.source == "dump" else "titles"
    dump_file = "page.sql.gz" if dump_kind == "dump" else "all-titles.gz"
    target_urls = []
    current_date = datetime.now(timezone.utc)
    for _ in range(args.months_back + 1):
        month_str = current_date.strftime("%Y%m01")
        target_urls.append(
            f"https://dumps.wikimedia.org/commonswiki/{month_str}/commonswiki-{month_str}-{dump_file}"
        )
        current_date = current_date.replace(day=1) - timedelta(days=1)

    if args.dry_run:
        if args.source in ("auto", "replica"):
            print(f"[DRY RUN] Would query replica {REPLICA_HOST} (replica.my.cnf present: {os.path.exists(REPLICA_CNF)})")
        if args.source in ("auto", "titles", "dump"):
            print("[DRY RUN] Would download from (first available):")
            for url in target_urls:
                print(f"  - {url}")
        print(f"[DRY RUN] Would write output to: {args.output}")
        if args.redirects_output:
            print(f"[DRY RUN] Would write redirects to: {args.redirects_output}")
        return 0

    # 1. Pick a source
    rows = None
    conn = None
    if args.source in ("auto", "replica"):
        conn = open_replica()
        if conn:
            print(f"Using replica {REPLICA_HOST}")
            rows = iter_replica(conn, args.namespace, args.prefix, args.verbose)
        elif args.source == "replica":
            print("Replica requested but unavailable.")
            return 1
    if rows is None:
        url = None
        if args.dump_dir:  # prefer a dump already saved locally
            url = next((u for u in target_urls if os.path.exists(local_dump(u, args.dump_dir))), None)
        url = url or find_dump(target_urls)
        if not url:
            print("Could not find any target month's dump file.")
            return 1
        print(f"Streaming {url}")
        rows = (iter_dump if dump_kind == "dump" else iter_titles)(url, args.namespace, args.prefix, args.verbose, args.dump_dir)

    # 2. Single pass: stream files and redirects to temp files, rename on success
    outputs = {False: args.output}
    if args.redirects_output:
        outputs[True] = args.redirects_output
    handles = {}
    counts = {False: 0, True: 0}
    try:
        for is_redirect, path in outputs.items():
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            handles[is_redirect] = open(path + ".tmp", "w", encoding="utf-8")

        for title, is_redirect in rows:
            if not pattern.match(title):
                continue
            counts[is_redirect] += 1
            if is_redirect in handles:
                handles[is_redirect].write(title + "\n")

        for h in handles.values():
            h.close()
        if conn:
            conn.close()

        if counts[False] == 0:
            print(f"No matching titles found (namespace {args.namespace}, prefix {args.prefix!r}, pattern {args.pattern!r}); keeping the previous output untouched.")
            print("Hint: files are in namespace 6, use -n 6. Note --pattern matches from the start of the title.")
            for path in outputs.values():
                os.remove(path + ".tmp")
            return 1

        for path in outputs.values():
            os.replace(path + ".tmp", path)
        print(f"Saved {counts[False]} titles to '{args.output}' ({counts[True]} redirects excluded).")
        if args.redirects_output:
            print(f"Saved {counts[True]} redirect titles to '{args.redirects_output}'.")
        return 0
    except Exception as e:
        print(f"Unexpected error while processing data stream: {e}")
        for h in handles.values():
            h.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())
