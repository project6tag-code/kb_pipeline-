"""GitHub documentation extraction (EXTRACTOR_VERSION): Markdown, reStructuredText, plain text.

Markdown/MDX keep their structure (headings, lists, tables, fenced code) and
lose only non-content: YAML/TOML front matter, MDX import/export lines, HTML
comments, badge/image syntax, reference-link definitions, and raw HTML tags
(their text is kept). Inline links keep their text. reStructuredText drops
directives that carry no text (images, badges, substitutions, comments) and
keeps everything else. Other documentation formats are kept as recorded.
"""

from __future__ import annotations

import html
import re

MARKDOWN_EXTENSIONS = frozenset({".md", ".markdown", ".mdx"})
RST_EXTENSIONS = frozenset({".rst"})
PLAIN_DOC_EXTENSIONS = frozenset({".txt", ".adoc", ".asciidoc", ".org", ".tex"})

_FRONT_MATTER = re.compile(r"\A(?:---\n.*?\n---|\+\+\+\n.*?\n\+\+\+)\n", re.S)
_HTML_COMMENT = re.compile(r"<!--.*?-->", re.S)
_MDX_IMPORT = re.compile(r"^(import|export)\s.*$", re.M)
_BADGE_LINK = re.compile(r"\[!\[[^\]]*\]\([^)]*\)\]\([^)]*\)")
_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_LINK = re.compile(r"(?<!!)\[([^\]]+)\]\((?:[^()\s]|\([^()\s]*\))*(?:\s+\"[^\"]*\")?\)")
_REF_LINK = re.compile(r"\[([^\]]+)\]\[[^\]]*\]")
_REF_DEF = re.compile(r"^\s{0,3}\[[^\]]+\]:\s+\S+.*$", re.M)
_HTML_TAG = re.compile(r"</?[A-Za-z][A-Za-z0-9-]*(?:\s[^<>]*)?/?>")
_FENCE = re.compile(r"^(```|~~~)")
_RST_DROP = re.compile(r"^\.\.\s+(image|figure|\|[^|]+\|\s+image|raw|include|meta|toctree|_[^:]+:|\w*$)", re.I)


def _outside_fences(text: str, transform) -> str:
    """Apply ``transform`` to non-code segments only (fenced code is kept verbatim)."""
    out, segment, in_fence = [], [], False
    for line in text.split("\n"):
        if _FENCE.match(line.lstrip()):
            if not in_fence:
                out.append(transform("\n".join(segment)))
                segment = []
                out.append(line)
            else:
                out.append("\n".join(segment))
                segment = []
                out.append(line)
            in_fence = not in_fence
            continue
        segment.append(line)
    out.append("\n".join(segment) if in_fence else transform("\n".join(segment)))
    return "\n".join(part for part in out)


def _markdown_prose(text: str) -> str:
    text = _HTML_COMMENT.sub("", text)
    text = _BADGE_LINK.sub("", text)
    text = _IMAGE.sub("", text)
    text = _LINK.sub(r"\1", text)
    text = _REF_LINK.sub(r"\1", text)
    text = _REF_DEF.sub("", text)
    text = _HTML_TAG.sub("", text)
    return html.unescape(text)


def extract_markdown(text: str, *, mdx: bool = False) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _FRONT_MATTER.sub("", text, count=1)
    if mdx:
        text = _MDX_IMPORT.sub("", text)
    text = _outside_fences(text, _markdown_prose)
    lines = [line for line in text.split("\n") if line.strip() not in ("[TOC]", "[[_TOC_]]")]
    return "\n".join(lines)


def extract_rst(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    out: list[str] = []
    skipping = False
    for line in text.split("\n"):
        if _RST_DROP.match(line):
            skipping = True
            continue
        if skipping and (line.startswith((" ", "\t")) or not line.strip()):
            if not line.strip():
                skipping = False
            continue
        skipping = False
        out.append(line)
    return "\n".join(out)
