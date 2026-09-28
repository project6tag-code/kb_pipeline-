# IDENTITY EXTENSION V1 — LOCKED

| | |
|---|---|
| Status | **LOCKED.** The implementation (`app/utils/ids.py`) and the frozen regression tests (`tests/unit/test_identity_extension.py`) agree. |
| Extends | Identity Specification V1 (document and chunk identities, **LOCKED, unchanged**) |
| Used by | Database Schema V1 (`docs/database_schema_v1.md`, sections 14.3 and 26) |
| Change policy | Any change to a prefix, input field, field order, validation rule or serialization requires **Identity Extension V2** (section 10). |

---

## 1. Purpose

Identity Extension V1 defines deterministic, reproducible UUIDv5 identities
for:

1. **Common Crawl source observations**: one immutable WARC record (capture)
   at a known byte range.
2. **GitHub source observations**: one file blob observed at one immutable
   repository commit.
3. **The Common Crawl Gold bridge dataset**: the pipeline-owned
   `public.datasets` row for the Common Crawl channel.
4. **The GitHub Gold bridge dataset**: the pipeline-owned `public.datasets`
   row for the public GitHub channel.

Observation identity answers **where** content was seen. Document and chunk
identity (Identity V1) answer **what** the content is. The two are never
mixed.

## 2. Relationship to the locked Identity Specification V1

- Identity V1 is **not modified**. Canonicalization, SHA-256, `KB_NAMESPACE`,
  the `document:v1:` and `chunk:v1:` prefixes, and `document_id`/`chunk_id`
  derivation are unchanged. All Identity V1 regression values still pass.
- This extension uses the **same namespace** with new, domain-separated name
  prefixes (section 7.4).
- Observation and bridge identities never depend on content, and content
  identities never depend on location.

## 3. Fixed KB namespace

```
KB_NAMESPACE = dd5af6b8-db26-4170-945d-557b16114d80
```

This is the same constant as Identity V1 (`app.utils.ids.KB_NAMESPACE`). No
other namespace is introduced. Every identity here is:

```
UUIDv5(KB_NAMESPACE, NAME)     -- RFC 4122 UUIDv5 (SHA-1 based), NAME encoded as UTF-8
```

## 4. Bridge dataset identity

```
bridge_dataset_id(source_name) = UUIDv5(KB_NAMESPACE, "dataset-bridge:v1:" + source_name)
```

| source_name | Exact UUIDv5 name string | Frozen UUID |
|---|---|---|
| `common_crawl` | `dataset-bridge:v1:common_crawl` | `a7d8d2e1-8400-5916-9440-b492e8e692ce` |
| `github` | `dataset-bridge:v1:github` | `b487f559-5f8e-56ba-9746-28658b9d09d1` |

- `source_name` is exactly the existing `public.dataset_sources.name` of the
  channel.
- Only the two values above are valid. Anything else is rejected, including
  case variants, surrounding whitespace and `commoncrawl`.
- The id is independent of the database's `dataset_sources` UUIDs. Those are
  looked up by name at bootstrap and are not portable.
- Function: `make_dataset_bridge_id(source_name: str) -> uuid.UUID`.

## 5. Common Crawl observation identity

One observation is one specific immutable WARC record, identified by its
location:

| Field | Meaning |
|---|---|
| `crawl_id` | Crawl identifier, e.g. `CC-MAIN-2024-33` |
| `warc_filename` | The WARC file's relative key exactly as published in the Common Crawl index, e.g. `crawl-data/CC-MAIN-2024-33/segments/…/warc/…warc.gz` |
| `warc_offset` | Byte offset of the (compressed) record within the WARC file |
| `warc_length` | Byte length of the (compressed) record |

```
NAME = "observation:v1:common_crawl:" + canonical_json({
          "crawl_id": crawl_id,
          "warc_filename": warc_filename,
          "warc_offset": warc_offset,
          "warc_length": warc_length
       })
```

All four fields are mandatory.

These are **excluded** from the identity: titles, extracted text, URLs,
publication dates, retrieval timestamps, local file paths, and the
transport (S3 or HTTPS).

Functions:

- `cc_observation_payload(crawl_id, warc_filename, warc_offset, warc_length) -> str`
  (validates the inputs and returns the canonical JSON);
- `cc_observation_id(...) -> uuid.UUID`.

## 6. GitHub observation identity

One observation is one file blob observed at one immutable commit.

| Field | Meaning |
|---|---|
| `repository_id` | GitHub **repository database id**: the numeric `id` in the REST API repository representation |
| `commit_sha` | Full object id of the commit |
| `path` | Repository-relative path exactly as it appears in the commit's git tree |
| `blob_sha` | Full object id of the blob at `path` in that commit |

```
NAME = "observation:v1:github:" + canonical_json({
          "repository_id": repository_id,
          "commit_sha": commit_sha,
          "path": path,
          "blob_sha": blob_sha
       })
```

**Frozen repository-identity choice: the repository database id (integer).**

- It is stable across repository renames and ownership transfers.
- It is a plain positive integer. It is trivial to validate and is stored
  directly as `kbp.github_repositories.repository_id bigint`.
- It is present in every REST repository payload used by discovery.
- The GraphQL **node id is not used**. GitHub has issued node ids in more than
  one encoding format over time (legacy base64 ids and newer prefixed ids such
  as `R_…`), so the same repository can be referred to by different node-id
  strings. A string-typed repository id is rejected outright, so the two
  representations cannot be silently switched.
- Owner/name is not used, because it changes on rename or transfer.

`blob_sha` is **mandatory**. It is part of the tree entry and can always be
recomputed from the fetched bytes as the git blob hash, so one observation can
never have two identities (with and without a blob).

Functions:

- `github_observation_payload(repository_id, commit_sha, path, blob_sha) -> str`;
- `github_observation_id(...) -> uuid.UUID`.

## 7. Exact canonical serialization

### 7.1 JSON payload

The payload is a JSON **object** whose keys appear in the **fixed order**
listed in sections 5 and 6, produced exactly as the reference implementation
does:

```
json.dumps(payload_dict, ensure_ascii=True, separators=(",", ":"), allow_nan=False)
```

The resulting byte and string rules are:

- **Key order:** exactly as listed. It is never sorted or reordered.
- **Whitespace:** none. `,` separates members and `:` separates key and
  value, with no spaces.
- **Strings:**
  - Standard JSON escaping: `\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`.
  - Other control characters (below U+0020) become `\u00XX` with lowercase
    hex.
  - Every non-ASCII code point becomes `\uXXXX` with lowercase hex. Code points
    above U+FFFF become a UTF-16 surrogate pair of escapes.
  - `/` is **not** escaped.
  - The payload is therefore pure ASCII.
- **Integers:** base-10, no sign for non-negative values, no leading zeros, no
  exponent, no decimal point.
- **No Unicode normalization, case folding or trimming** is applied to any
  field before serialization.

### 7.2 UUIDv5 name

```
NAME  = PREFIX + PAYLOAD                    (a Python str)
bytes = NAME encoded as UTF-8               (identical to ASCII, since NAME is pure ASCII)
id    = UUIDv5(KB_NAMESPACE, bytes)
```

For bridge ids, `PAYLOAD` is the bare `source_name` token (section 4). It
needs no JSON, because it is restricted to two fixed ASCII values.

### 7.3 Why the serialization is unambiguous

- JSON string quoting and escaping make field boundaries unambiguous for every
  possible field value, including values that contain `"`, `,`, `:`, `/`,
  newlines or non-ASCII text.
- Named keys in a fixed order mean no two different inputs serialize to the
  same payload.
- Naive delimiter concatenation is not used anywhere.

### 7.4 Prefix registry (all in `KB_NAMESPACE`)

| Prefix | Status |
|---|---|
| `document:v1:` | Identity V1, **LOCKED** |
| `chunk:v1:` | Identity V1, **LOCKED** |
| `observation:v1:common_crawl:` | Identity Extension V1, **LOCKED** |
| `observation:v1:github:` | Identity Extension V1, **LOCKED** |
| `dataset-bridge:v1:` | Identity Extension V1, **LOCKED** |

- Each prefix begins with a distinct domain word (`document`, `chunk`,
  `observation`, `dataset-bridge`).
- The two observation prefixes differ in their channel segment, and every
  prefix ends with `:`.
- Identity V1 names continue with a hex digest; observation names continue
  with `{`.
- Therefore no name in one domain can equal a name in another.

## 8. Validation rules

Validation happens **before** serialization. Invalid input raises; nothing
is silently normalized.

| Rule | Error |
|---|---|
| A field has the wrong type (for example `bytes`, `None` or `float`; **`bool` is rejected wherever an int is expected**) | `TypeError` |
| An empty or malformed value | `ValueError` |

### 8.1 Bridge

- `source_name` must be a `str`, exactly `common_crawl` or `github`.

### 8.2 Common Crawl

| Field | Rule |
|---|---|
| `crawl_id` | non-empty `str`; must fully match `CC-MAIN-[0-9]{4}-[0-9]{2}` (the form stored in `kbp.cc_crawls`) |
| `warc_filename` | non-empty `str`; no whitespace or control characters; must start with `crawl-data/<crawl_id>/` (a relative index key: no scheme, host, leading `/` or transport prefix) |
| `warc_offset` | `int` (not `bool`), `0 ≤ value ≤ 2^63−1` |
| `warc_length` | `int` (not `bool`), `1 ≤ value ≤ 2^63−1` |

### 8.3 GitHub

| Field | Rule |
|---|---|
| `repository_id` | `int` (not `bool`, not `str`), `1 ≤ value ≤ 2^63−1` |
| `commit_sha` | non-empty `str`, **exactly 40 or 64 lowercase hex** characters (git SHA-1 or SHA-256 object format). Uppercase is **rejected**, not lowercased. |
| `blob_sha` | same rule as `commit_sha`, **and the same length** as `commit_sha` (one repository uses one object format) |
| `path` | see below |

`path` rules. The path must be the repository-relative git tree path, taken
verbatim:

- a non-empty `str`;
- no leading `/` and no trailing `/`;
- no empty segment (`//`) and no `.` or `..` segment (git trees cannot
  contain these);
- no U+0000 and no surrogate code points: the path must be valid Unicode, so
  its UTF-8 form exists. Non-UTF-8 git paths are therefore out of scope for V1
  and are dead-lettered by acquisition.
- **Accepted verbatim:** case, spaces, backslashes, leading dots
  (`.github/…`, `..hidden`), non-ASCII characters in either NFC or NFD. They
  are never converted, resolved, case-folded or normalized. Two paths that
  differ in any code point are different observations.

## 9. Frozen regression vectors

These are hard-coded in `tests/unit/test_identity_extension.py`. The tests
check each UUID both through the implementation and through `uuid.uuid5`
applied directly to the literal name string.

### 9.1 Bridge datasets

| Input | NAME | UUID |
|---|---|---|
| `common_crawl` | `dataset-bridge:v1:common_crawl` | `a7d8d2e1-8400-5916-9440-b492e8e692ce` |
| `github` | `dataset-bridge:v1:github` | `b487f559-5f8e-56ba-9746-28658b9d09d1` |

### 9.2 Common Crawl observation

Input:

```
crawl_id      = "CC-MAIN-2024-33"
warc_filename = "crawl-data/CC-MAIN-2024-33/segments/1722640353668.0/warc/CC-MAIN-20240805230130-20240806020130-00042.warc.gz"
warc_offset   = 123456789
warc_length   = 18220
```

Canonical payload:

```
{"crawl_id":"CC-MAIN-2024-33","warc_filename":"crawl-data/CC-MAIN-2024-33/segments/1722640353668.0/warc/CC-MAIN-20240805230130-20240806020130-00042.warc.gz","warc_offset":123456789,"warc_length":18220}
```

The NAME is `observation:v1:common_crawl:` followed by the payload above.

**UUID: `afb1a4e1-acd0-5e13-96e2-94eb6aec8299`**

### 9.3 GitHub observation (SHA-1, ASCII path)

Input:

```
repository_id = 123456789
commit_sha    = "0123456789abcdef0123456789abcdef01234567"
path          = "docs/intro.md"
blob_sha      = "89abcdef0123456789abcdef0123456789abcdef"
```

Canonical payload:

```
{"repository_id":123456789,"commit_sha":"0123456789abcdef0123456789abcdef01234567","path":"docs/intro.md","blob_sha":"89abcdef0123456789abcdef0123456789abcdef"}
```

The NAME is `observation:v1:github:` followed by the payload above.

**UUID: `bf308fc0-77e3-5540-bcf0-5def36127e04`**

### 9.4 GitHub observation (non-ASCII path)

The input is the same as 9.3, but with `path = "docs/Café Überblick.md"`
(`é` = U+00E9, `Ü` = U+00DC, both precomposed).

Canonical payload:

```
{"repository_id":123456789,"commit_sha":"0123456789abcdef0123456789abcdef01234567","path":"docs/Café Überblick.md","blob_sha":"89abcdef0123456789abcdef0123456789abcdef"}
```

**UUID: `7ce8865a-f6d9-54d1-8933-6989729e5583`**

### 9.5 GitHub observation (SHA-256 object format)

Input:

```
repository_id = 987654321
commit_sha    = "a" * 64
path          = "src/lib.rs"
blob_sha      = "b" * 64
```

Canonical payload:

```
{"repository_id":987654321,"commit_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","path":"src/lib.rs","blob_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
```

**UUID: `4119c85b-bde5-55b9-b978-194249414444`**

The inputs in 9.2–9.5 are synthetic fixtures chosen to exercise the format.
They are not claims about real Common Crawl records or GitHub repositories.

## 10. Versioning policy

- Identity Extension V1 is **LOCKED**. The following are breaking changes and
  require a **V2** with new prefixes (for example `observation:v2:…`):
  - any change to a prefix, a field name, the field order, the set of identity
    inputs, a validation rule that changes which inputs are accepted, or the
    serialization;
  - changing the GitHub repository identity choice;
  - adding a bridge source.
- V1 identities remain valid forever for the rows they identify. A V2 would
  coexist with V1 and never re-key existing rows in place.
- The frozen vectors in section 9 must never be edited to make a changed
  implementation pass.
- `KB_NAMESPACE` is shared with Identity V1 and is permanently frozen.

## 11. Explicit non-goals

Identity Extension V1 does **not**:

- change Identity V1 (canonicalization, `document_id`, `chunk_id`, or their
  frozen vectors);
- canonicalize, resolve or validate URLs, or treat URLs as identity;
- fetch anything, verify that a crawl, WARC file, repository, commit or blob
  exists, or check repository visibility or license (acquisition and policy
  concerns);
- identify documents or chunks by source location;
- identify other channels (Wikipedia enters only through Common Crawl
  captures);
- support non-UTF-8 git paths or GitHub node ids;
- touch PostgreSQL, migrations or Alembic.
