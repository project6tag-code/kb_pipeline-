# Common Crawl Fact KB V2 — sources, strict preprocessing, source ids and dates

| | |
|---|---|
| Status | Implemented and unit-tested offline. Migration 001E applied to `kb_pipeline_test`. Probed on real CC-MAIN-2024-33 captures (§7). The public-table patch and the content reset (§6) are operator steps that must be run as `postgres`. |
| Scope | Common Crawl only (GitHub stays available with `--sources cc,github`, but it is not the default). `kb_pipeline_test` only. |
| Supersedes | The Gold metadata mapping of Gold Publisher V1 (one shared bridge `source_id`, `published_at`/`updated_at` NULL). |

## 1. Why

Before this change, Gold held 6,289 rows. Nearly all the Common Crawl rows came from raw-IP hosts (shop categories, "Terms and Conditions", event lists). Every row had `published_at` and `updated_at` set to NULL, and all CC rows shared one `source_id` (the "Common Crawl" bridge dataset). There were two root causes:

- `cluster.idx` is sorted by SURT key, and keys that start with a digit (raw IPv4 hosts) sort first. A bounded pilot (`--cc-partitions 1`) therefore always read the IP region.
- The candidate filter checked only status, MIME, language and size. Nothing looked at the host, the URL or the page type.

## 2. Source selection (Common Crawl Acquisition, filter `f2`)

There are two **profiles**. Each one is part of the filter version, and therefore of the job identity.

| profile | planning | candidate filter |
|---|---|---|
| `reference` (default for `KB_CC_PROFILE`) | Only `cluster.idx` blocks whose key range can contain a listed domain (SURT host prefixes such as `org,wikipedia,en)`), so only those index ranges are read | Host must be a listed domain or one of its subdomains, plus the URL policy |
| `open` | Every block outside the raw-IP region; `--cc-open-stride` samples every Nth eligible partition | URL policy |

- The curated list lives in [reference_domains_v1.json](../app/sources/commoncrawl/reference_domains_v1.json): English Wikipedia, Britannica, SEP, IEP, NIH/NCBI, MedlinePlus, CDC, WHO, NASA, NOAA, USGS, NIST, LOC, the Smithsonian, OpenStax, LibreTexts and others. Edit the file, or set `KB_CC_REFERENCE_DOMAINS=a.org,b.gov`. Any change produces a new filter version and new jobs; nothing already acquired is rewritten.
- Reference partitions get job priority 50 and open partitions 100. Lower is claimed first, so the reference publishers are always processed first.

**URL policy `url2`** (`app/sources/commoncrawl/index.py:url_rejection`). It is evaluated on the index row, before anything is fetched, and rejects:

- site homepages (`/`, `/index.html`, `/home` …), which are navigation or news hubs rather than articles;
- raw IP hosts, non-default ports, punycode hosts and spam-shaped hosts;
- URLs longer than 300 characters or deeper than 8 path segments;
- path segments that name non-content pages:
  - commerce (`cart`, `shop`, `product(s)` …);
  - accounts and admin (`login`, `wp-admin` …);
  - listings and feeds (`search`, `tag`, `category`, `archive`, `feed`, `/page/N`);
  - legal and corporate boilerplate (`terms`, `privacy`, `cookie-policy`, `contact` …);
  - **test, sample and synthetic areas** (`test`, `sandbox`, `demo`, `sample`, `example(s)`, `dummy`, `staging`, `lorem-ipsum`, `placeholder`);
- MediaWiki non-article namespaces (`Talk:`, `User:`, `Special:`, `File:`, `Category:` …);
- data files (`.xml`, `.json`, `.rss`, `.atom`, `.txt`, `.csv`);
- query strings with more than 2 parameters, or with tracking, session, sort, paging or edit keys (`utm_*`, `action`, `oldid`, `page` …).

## 3. Strict Silver preprocessing (policy label changes automatically)

The 22 gates are unchanged in structure (`docs/silver_preprocessing_v1.md`). Gate 16 (noise) is now `noise2+ptype1`:

- **`noise2`**: `low_information` is measured over a fixed 1,000-word window. In `noise1` the whole-text unique-word ratio always fell with length, and that rejected a real 17k-word Stanford Encyclopedia of Philosophy entry.
- **`ptype1`** (`app/processing/quality/page_type.py`, Common Crawl pages only). Any hit rejects the document (`rejected_quality`):

| reason | rule |
|---|---|
| `non_content_url:*` | URL policy `url2` (also re-checked here for pages acquired under older filters) |
| `non_content_title` | Title is site chrome: shopping cart, terms of use, privacy policy, search results, "Page 3 of 12", monthly archives, log in, 404 … Topic words such as *Register*, *Deal* or *Archives* alone do not trigger it. |
| `schema_type:<Type>` | The page declares itself in JSON-LD as a `Product`, `Offer`, `AggregateOffer`, `Store`, `LocalBusiness`, `Restaurant`, `Hotel`, `RealEstateListing`, `JobPosting`, `Event`, `SearchResultsPage`, `CheckoutPage`, `ProfilePage`, `DiscussionForumPosting`, `QAPage` or `SocialMediaPosting` |
| `commerce_page` | At least 3 shopping or marketplace phrases, or at least 6 prices per 1,000 words |
| `listing_page` | At least 4 "read more" / "continue reading" teasers |
| `forum_or_comments` | At least 5 forum or comment markers, at a rate of at least 6 per 1,000 words |
| `synthetic_or_placeholder` | AI-assistant boilerplate ("as an AI language model"), AI-generation notices ("this article was generated by AI"), lorem ipsum, dummy, sample or placeholder text, WordPress "Hello world", unfilled template slots (`[insert …]`, `{{name}}`, `%%var%%`, `${var}`). Mentioning ChatGPT as a topic does not trigger it. |

### 3.1 Emojis and symbols (`sym1`, Silver, before canonicalization)

[app/processing/normalization/symbols.py](../app/processing/normalization/symbols.py) runs on the extracted text of every prose, HTML or Markdown document, before the locked canonicalizer, so the document id is computed from the cleaned text. Code and config files, and text inside fenced code, are never touched.

| removed | examples |
|---|---|
| emojis, pictographs, flags, skin tones, keycaps | 😀 🔥 ✅ ⭐ 🇺🇸 👍🏽 1️⃣ |
| dingbats, decorative shapes, ™ ® | ★ ✔ ➤ ► ■ ● |
| icon-font glyphs | Private Use Area characters |
| invisible characters | zero-width space and joiners, soft hyphen, bidi controls |
| bullets | `•` at a line start becomes a `- ` list item; elsewhere it becomes a space |
| decorative runs and symbol-only lines | `=====`, `____` form blanks, `.....`, `* * *` |
| hashtag signs | `#climate` → `climate` |
| footnote debris | `^` back-links, `100,000^`, `Album*`, `(*)`, `† ‡` |

Symbols that carry meaning are **kept**: `US $7.99`, `50%`, `C#`, `#1`, `snake_case`, `25 °C`, `N·m`, `x → y`, `2*3`, `x^2`, `R&D`, and logic or math notation.

### 3.2 Plain-text chunks for Gold (chunking `c2`, `plain2`)

Silver text keeps its structure markers (`#` headings, `-` lists, `a | b` tables, `>` quotes, code fences), because the chunker splits on them. The **stored chunk text** is rendered as plain text by [app/processing/chunking/plain.py](../app/processing/chunking/plain.py). That text is hashed into `chunk_id`, embedded, and copied byte-for-byte into `public.kb.chunk`.

| Silver span | stored chunk text |
|---|---|
| `## Physical properties` | `Physical properties` |
| `- Colourless gas` | `Colourless gas` (numbered steps keep `3.`) |
| `Melting point \| 54.36 K` / `A \| B \| C` | `Melting point: 54.36 K` / `A; B; C` |
| `> quote`, code fences | `quote`; fences dropped, code kept verbatim |
| `**bold**`, `_it_`, `` `x` ``, `[text](url)`, `![alt](img)` | `bold`, `it`, `x`, `text`, `alt` |
| `[5]`, `[a]`, `[citation needed]`, `[edit]` | removed (`x[0]` and `[Edit]` are kept) |

- **New invariant** (replaces the `c1` rule): `chunk_text == canonicalize_text(plain.render(silver_text[char_start:char_end], kind))`. The spans in `kbp.chunk_occurrences` still point into the Silver text.
- **Fact guard `fg-v2`** (chunk dedup method `oph-w3-b10r4-j70-v2`): plain section-title lines are treated as context, as `#` headings were before. Otherwise "Kariba Dam" vs "Zambezi scheme" titles would keep real near-duplicates apart. A title is a standalone line of at most 12 words with no digit and no sentence punctuation; lines with numbers are always compared.
- **Registered on `kb_pipeline_test`** (migrator): chunking version 5 `kbp-chunker-c2-48b7b01539` and dedup method 5 `oph-w3-b10r4-j70-v2`.
  - Version 4 (`kbp-chunker-c2-8701104829`) was registered during development and is unused.
  - The `c1` / `v1` rows stay as history.
- **Checked on real CC-MAIN-2024-33 pages** (Wikipedia, Britannica, SEP): accepted chunks contain no Markdown marker lines and no emoji or footnote symbols. SEP keeps its logic notation, as intended.

**Thresholds (stricter):** `noise_min_words` 40 → **80**; `quality_threshold` 0.55 → **0.60**; `suitability_pass` 0.55 → **0.60**; `suitability_fail` 0.35 → **0.40**. UNRESOLVED suitability is still quarantined, never accepted.

**No synthetic data is downloaded.** Only real archived captures of real URLs are fetched, and test and sample URL areas are never planned. Synthetic or placeholder text that slips through is rejected by `ptype1`. Test fixtures exist only in `tests/` and are never written to Bronze or the database outside the opt-in integration tests (which use a random `it-<hex>/` prefix and need a separate database URL).

## 4. Gold metadata (Gold Publisher V2)

Each new Gold row is published per chunk, in one transaction, in this order: `public.datasets` (once per source document) → `public.metadata` → `public.kb` → `kbp.gold_publications`.

| column | value |
|---|---|
| `source_id` | **The original source document.** CC: the capture's own **WARC-Record-ID** (a UUID that the archive assigned to that archived document). GitHub: UUIDv5(`KB_NAMESPACE`, `github-blob:v1:<blob_sha>`). Every chunk of one document shares it; different documents never do. |
| `published_at` | The page's declared publication date; if the page declares none, the **crawl date** |
| `updated_at` | The first of: declared modification date, HTTP `Last-Modified`, declared publication date, crawl date — skipping any value earlier than `published_at` |
| `crawled_at` (new column) | The capture's `WARC-Date` |

How the CC dates are found (`app/sources/commoncrawl/dates.py`, `ccdate-v1`):

- **Evidence**, strongest first: JSON-LD `datePublished` / `dateCreated` / `dateModified`; then `<meta>` tags (`article:published_time`, `citation_publication_date`, `dcterms.issued`, `dcterms.modified`, `og:updated_time` …); then `<time itemprop=…>`; and finally the HTTP `Last-Modified` header.
- **Scope**: the whole page is scanned, up to 8 MiB, because Wikipedia writes its JSON-LD at the very end.
- **Validation**: a date earlier than 1990, later than crawl + 1 day, or a modification date earlier than the publication date is discarded, never clamped.
- **Never used**: dates in the URL or in the text.
- **Where the evidence lives**: it is stored per observation in `kbp.source_dates` (Migration 001E, insert-only). Which rule produced each Gold date is recorded in the dataset row (`published_from`, `updated_from`, e.g. `jsonld:datePublished`, `http:last-modified`, `crawl_date_fallback`).

**`public.datasets` source-document rows.** `metadata.source_id` must reference `public.datasets(id)` (the FK is kept), so the publisher inserts one row per original document:

| column | value |
|---|---|
| `id` | `source_id` |
| `source_id` | The channel's `dataset_sources` parent |
| `dataset_name` | Title, or the URL (at most 255 characters) |
| `status` | `KB_SOURCE_DOCUMENT` |
| `metadata` | URL, WARC-Record-ID, crawl id, WARC filename, offset, length, payload digest, crawl time and date evidence (GitHub: blob SHA, repository id, commit and path) |

The row is inserted once and verified on every later chunk; a mismatch is a `GoldConflict`, never an overwrite. The two `KB_REFERENCE` bridge rows remain but are no longer used as `source_id`.

## 5. Schema and privileges

- **Migration 001E** (`kbp_001e`, applied) adds `kbp.source_dates`. The runtime role gets SELECT and INSERT; CHECK constraints keep the evidence consistent. Every component now expects `kbp_001e`.
- **Admin patch** [scripts/db/admin_gold_v2_patch.sql](../scripts/db/admin_gold_v2_patch.sql), run as `postgres`, is idempotent:
  - it adds the nullable column `public.metadata.crawled_at timestamptz`;
  - it grants `SELECT, INSERT` on `public.datasets` to `kb_pipeline_app` (no UPDATE or DELETE).

  This deliberately widens the V1 privilege rule (privileges §11), because a per-document `source_id` must exist in `public.datasets`.

## 6. Operator steps (once, in this order)

```powershell
# 0. stop every pipeline process
# 1. backup (postgres; credentials from pgpass)
pg_dump -h localhost -U postgres -Fc -f backup\kb_pipeline_test_before_v2_reset.dump kb_pipeline_test
# 2. schema (already done: kbp_001e)
$env:KB_MIGRATION_DATABASE_URL = 'postgresql+psycopg://kb_pipeline_migrator@localhost:5432/kb_pipeline_test'
.\.venv\Scripts\python.exe -m alembic -x expect_database=kb_pipeline_test upgrade head
# 3. Gold V2 public patch
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d kb_pipeline_test -f scripts/db/admin_gold_v2_patch.sql
# 4. content reset (destructive, needs the explicit confirmation)
psql -X -v ON_ERROR_STOP=1 -v confirm=RESET_KB_PIPELINE_TEST -h localhost -U postgres -d kb_pipeline_test `
     -f scripts/db/admin_reset_kb_pipeline_test.sql
# 5. derived local state (rebuildable; stale entries would point at deleted documents)
Remove-Item -Recurse -Force data\bronze\commoncrawl\*, data\bronze\github\*, data\chunk_dedup\*
Remove-Item -Force data\silver\near_index.sqlite3*
# 6. run (Common Crawl only, reference publishers first)
$env:KB_DATABASE_URL = 'postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test'
.\.venv\Scripts\python.exe -m app.pipeline.cli run --expect-database kb_pipeline_test --cc-partitions 2 --cc-iterations 6
```

The reset keeps the schema, roles, `kbp.alembic_version`, the frozen registrations (`dedup_methods`, `chunking_versions`, `embedding_models` with model 2 `active_gold`, `source_dataset_map`), `public.dataset_sources` and the two `KB_REFERENCE` rows. The Silver near-dedup method row is kept. The new `noise2+ptype1` policy is only a label, so no re-registration is needed.

**Checks after a run:**

```sql
SELECT count(*) AS rows, count(published_at), count(updated_at), count(crawled_at), count(DISTINCT source_id)
FROM public.metadata;                                                     -- the three date counts = rows
SELECT d.status, count(*) FROM public.datasets d GROUP BY 1;              -- KB_SOURCE_DOCUMENT = distinct source_id
SELECT o.url_host, count(*) FROM kbp.document_registry r
JOIN kbp.source_observations o ON o.observation_id = r.first_observation_id
WHERE r.status = 'accepted' GROUP BY 1 ORDER BY 2 DESC LIMIT 20;           -- reference publishers first
```

`python -m app.processing.gold.cli status --expect-database kb_pipeline_test` audits every ledger row. A row fails the audit if:

- its `source_id` is not its WARC-Record-ID, or is a bridge id;
- its `datasets` row is missing;
- any of its three dates is NULL, or `updated_at < published_at`;
- `crawled_at` differs from `kbp.source_dates`.

## 7. Real-data probe (CC-MAIN-2024-33, read-only, nothing written to the database)

| page | filter | dates found | Silver |
|---|---|---|---|
| britannica.com/science/photosynthesis | accept | published 1998-07-20, modified 2024-07-25 (JSON-LD) | accepted gates (suitability 0.93) |
| plato.stanford.edu classical logic | accept | issued 2000-09-16, modified 2022-06-29 (`dcterms`) | `noise1` rejected it as `low_information`; fixed in `noise2` |
| cdc.gov/flu/about | accept | published 2023-05-02 (`article:published_time`) | accepted gates |
| nasa.gov | accept | JSON-LD published and modified | accepted gates |
| en.wikipedia.org article | accept | JSON-LD at byte 293,747 of 294,509: missed by the 256 KiB scan, fixed by the full-page scan | accepted gates (suitability 1.00) |
| ebay.com/itm/… | open only | none (crawl-date fallback) | passed every V1 gate; now rejected (`schema_type:Product`, `commerce_page`) |

## 8. Limits

- Only dates the publisher declared are used. Pages that declare none get the crawl date as `published_at`; the dataset row says so (`crawl_date_fallback`).
- The page-type rules are deterministic heuristics. They are versioned, and the reasons are kept on every rejected document (`document_registry.status_reason`). Review the rejected and quarantined counts after each run and tune through a new version, never in place.
- `public.metadata.crawled_at` and the `public.datasets` INSERT grant exist only on `kb_pipeline_test`. Production needs its own compatibility review.
