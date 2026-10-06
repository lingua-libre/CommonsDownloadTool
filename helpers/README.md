# Helpers: titles tools

Run the commands from the CommonsDownloadTool root (outputs go to `./tmp/`).

Small tools to fetch Wikimedia Commons titles from dumps or the replica and filter them.

## Scripts

### titles_fetcher.py
Lists page titles of a Commons namespace (`-n`, default 0; 6 = files), optionally narrowed by a prefix (`-x`) and a regex (`-p`), via two interchangeable sources:

- `replica`: live Commons replica (Toolforge/PAWS; needs `~/replica.my.cnf` and `pip install pymysql`). Fast and current, run daily.
- `titles`: streams `commonswiki-<date>-all-titles.gz` (~1.5 GB, stdlib only). Redirects are kept; `commons_download_tool.py` skips them at download time (404, see `--redirects`). Use this on a plain VPS, see the LinguaLibre_data README-wmcloud.md.
- `dump`: streams `commonswiki-<date>-page.sql.gz` (~7 GB, stdlib only), run monthly.
- `auto` (default): replica if reachable, else titles.

`redirect.sql.gz` is not used: it maps redirect page ids to *targets*, and `all-titles.gz` has no ids, so they can't be joined. `page.sql.gz` carries id, namespace, title and `page_is_redirect` together.

```bash
python3 helpers/titles_fetcher.py                                  # all main-namespace titles (no prefix, no pattern)
python3 helpers/titles_fetcher.py -n 6 -x LL-Q                     # auto source, titles starting with LL-Q
python3 helpers/titles_fetcher.py -n 6 -x LL-Q -s replica                # replica only
python3 helpers/titles_fetcher.py -n 6 -x LL-Q -s dump -r ./tmp/redirects.txt   # dump, also save redirect titles
python3 helpers/titles_fetcher.py -n 6 -x LL-Q -d ./dumps   # keep the dump in ./dumps (reused next run, not downloaded again)
python3 helpers/titles_fetcher.py -n 6 -x LL-Q -p '^LL-Q\d{3,9}_.+\.(?:wav|ogg)$'  # custom pattern (underscore title form)
```

Cron:

```cron
# daily, replica host
15 3 * * *  cd /path/to/CommonsDownloadTool && python3 helpers/titles_fetcher.py -n 6 -x LL-Q -s replica
# monthly, dump (published on the 1st; page.sql.gz may take days, older months are tried as fallback)
0 6 5 * *   cd /path/to/CommonsDownloadTool && python3 helpers/titles_fetcher.py -n 6 -x LL-Q -s dump
```

With `--dump-dir`, the dump is saved as `<dir>/<name>.gz` once fully read (a `.part` file is removed on failure) and reused on later runs, so changing `-n`, `-x` or `-p` doesn't re-download ~1.5 GB (`titles`) or ~7 GB (`dump`).

Output is written to a `.tmp` file and renamed, so a failed run keeps the previous list.

### titles_filterer.py
Filters a file containing one title per line by regex and writes matches to a target file.

```bash
python3 helpers/titles_filterer.py -i ./titles.txt -p '/Q117707514/' -o ./titles-filtered.txt  # filter by QID
python3 helpers/titles_filterer.py -i ./titles.txt -p 'Q117707514' -o ./titles-filtered.txt  # same without slashes
python3 helpers/titles_filterer.py --input ./tmp/titles.txt --regex 'LL-Q\d+' --output ./tmp/filtered_titles.txt  # general LL pattern
```

## Using the replica on Toolforge (wmcloud.org)

The replica is only reachable from **Toolforge** (and PAWS), not from a plain Cloud VPS instance. On a VPS, use `-s dump`. The steps below are the standard Toolforge setup; adapt tool and path names.

1. **Get a tool account.** Create a Toolforge account and a tool at https://toolsadmin.wikimedia.org/ (e.g. `lingualibre-titles`).
2. **Log in and switch to the tool:**
   ```bash
   ssh <shell-user>@login.toolforge.org
   become lingualibre-titles
   ```
3. **Check credentials.** Toolforge creates `~/replica.my.cnf` (user + password for the replicas) in the tool's home. The script reads it automatically.
   ```bash
   ls -l ~/replica.my.cnf
   ```
4. **Get the code and a virtualenv with `pymysql`:**
   ```bash
   git clone <your-repo-url> ~/CommonsDownloadTool
   toolforge webservice python3.11 shell      # opens a shell in the matching image
   python3 -m venv ~/venv && ~/venv/bin/pip install pymysql
   exit
   ```
5. **Test the connection** (host `commonswiki.analytics.db.svc.wikimedia.cloud`, database `commonswiki_p`):
   ```bash
   mysql --defaults-file=~/replica.my.cnf -h commonswiki.analytics.db.svc.wikimedia.cloud commonswiki_p \
     -e "SELECT COUNT(*) FROM page WHERE page_namespace=6 AND page_title LIKE 'LL-Q%' AND page_is_redirect=0"
   ```
   The `analytics` host suits long scans like this one; `commonswiki.web.db.svc.wikimedia.cloud` is for short interactive queries.
6. **Run the fetcher once:**
   ```bash
   ~/venv/bin/python ~/CommonsDownloadTool/helpers/titles_fetcher.py -n 6 -x LL-Q -s replica -v
   ```
7. **Schedule it daily with the Toolforge jobs service** (replaces crontab on Toolforge):
   ```bash
   toolforge jobs run ll-titles \
     --command "cd ~/CommonsDownloadTool && ~/venv/bin/python helpers/titles_fetcher.py -n 6 -x LL-Q -s replica" \
     --image python3.11 --schedule "15 3 * * *"
   toolforge jobs list
   ```
   Logs go to `~/ll-titles.out` and `~/ll-titles.err`.

Notes:
- Replica data lags the live wiki by seconds to minutes; the monthly dump is the fallback, not the primary.
- Replica queries are killed after a time limit; the script already paginates on `page_id` in batches of 100k.
- Never copy `replica.my.cnf` elsewhere or commit it.
- Docs: https://wikitech.wikimedia.org/wiki/Help:Toolforge and https://wikitech.wikimedia.org/wiki/Help:Wiki_Replicas

## Full workflow

```bash
python3 helpers/titles_fetcher.py -s titles -n 6 -x 'LL-Q' -p '^LL-Q\d{3,9}[_ ].+\.(?:wav|ogg)$'  # fetch matching titles to ./tmp/titles.txt
python3 helpers/titles_filterer.py -i ./tmp/titles.txt -p 'Q117707514' -o ./tmp/filtered.txt  # narrow down, e.g. one language
python3 commons_download_tool.py --titles ./tmp/filtered.txt --output out.zip  # download and zip
```

## Notes

- All scripts are standard-library Python 3 tools (`commons_download_tool.py` also needs `requests`).
- Output files are written in the current directory unless a path is specified explicitly.
- Back to the [main README](../README.md). Lingua Libre specific wrappers (per-language archives, QIDs) live in the LinguaLibre_data repo.
