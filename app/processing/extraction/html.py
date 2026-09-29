"""Gates 05-10: HTML structural parsing and strong cleaning (HTML_CLEANER_VERSION).

Pipeline (deterministic; stdlib ``html.parser`` only):

* 05 parse   - a bounded, tolerant element tree (node and depth limits, HTML
               implicit-close rules for p/li/dt/dd/tr/td/th/option).
* 06 active  - script, style, noscript, template, svg, math, canvas, iframe,
               object/embed, media, form controls, <head>; hidden content
               (``hidden``, ``aria-hidden=true``, ``display:none``,
               ``visibility:hidden``, screen-reader-only classes).
* 07 boiler  - semantic page chrome: nav, aside, site header/footer, and ARIA
               roles navigation/banner/contentinfo/complementary/search/...
               (a header/footer inside an article or main is content).
* 08 chrome  - class/id tokens of menus, breadcrumbs, sidebars, cookie/consent
               banners, share/social bars, login/signup, comments, related
               links, newsletter prompts, pagination, search, widgets;
               link-dense blocks; standalone chrome phrases; repeated
               template blocks.
* 09 ads     - ad/sponsor/promo containers and standalone ad labels.
* 10 struct  - main-content selection (main / article / content container,
               only when it holds a substantial share of the text) and
               structure reconstruction: headings as ``#``, lists as ``-``/``N.``,
               meaningful tables as ``a | b`` rows, preformatted/code blocks
               fenced verbatim, blockquotes, definition lists, captions.

A protected element (one holding most of the page text, or an ``h1`` for
header-like tokens) is never removed by a class/id heuristic.
"""

from __future__ import annotations

import html as html_lib
import re
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from html.parser import HTMLParser

VOID = frozenset({"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source",
                  "track", "wbr", "keygen"})
ACTIVE = frozenset({"script", "style", "noscript", "template", "svg", "math", "canvas", "iframe", "object",
                    "embed", "applet", "audio", "video", "map", "select", "option", "button", "input",
                    "textarea", "dialog", "head", "link", "meta", "base", "frameset", "frame", "noembed",
                    "noframes", "picture", "source", "track", "datalist", "output", "progress", "meter"})
SEMANTIC_CHROME = frozenset({"nav", "aside", "menu"})
CHROME_ROLES = frozenset({"navigation", "banner", "contentinfo", "complementary", "search", "menu", "menubar",
                          "toolbar", "dialog", "alertdialog", "tablist", "form"})
BLOCK = frozenset({"address", "article", "aside", "blockquote", "body", "caption", "center", "dd", "details",
                   "div", "dl", "dt", "fieldset", "figcaption", "figure", "footer", "form", "h1", "h2", "h3", "h4",
                   "h5", "h6", "header", "hr", "html", "li", "main", "nav", "ol", "p", "pre", "section",
                   "summary", "table", "tbody", "td", "tfoot", "th", "thead", "tr", "ul", "#root"})
HEADINGS = {f"h{i}": i for i in range(1, 7)}
HIDDEN_CLASS_NAMES = frozenset({"hidden", "hide", "invisible", "sr-only", "sr_only", "visually-hidden",
                                "visuallyhidden", "screen-reader-text", "screenreader-text", "screen-reader-only",
                                "a11y-hidden", "offscreen", "u-hidden", "is-hidden"})
CHROME_TOKENS = frozenset({
    "nav", "navbar", "navigation", "navlinks", "menu", "menubar", "submenu", "megamenu", "dropdown",
    "breadcrumb", "breadcrumbs", "crumbs", "sidebar", "sidebars", "rail", "footer", "header", "masthead",
    "topbar", "toolbar", "pagination", "pager", "paging", "share", "sharing", "sharebar", "social", "socials",
    "comment", "comments", "disqus", "related", "recommended", "recommendations", "newsletter", "subscribe",
    "subscription", "signup", "login", "signin", "logout", "register", "registration", "account", "cookie",
    "cookies", "consent", "gdpr", "ccpa", "cmp", "banner", "popup", "modal", "overlay", "lightbox", "skip",
    "skiplink", "search", "searchbox", "searchform", "widget", "widgets", "tagcloud", "tags", "rating",
    "ratings", "vote", "feedback", "backtotop", "print", "follow", "byline", "authorbox", "bio", "toc",
    "copyright", "legal", "sitemap", "promo", "cta", "trending", "popular", "morestories", "readmore",
})
HEADER_LIKE = frozenset({"header", "masthead", "banner", "topbar"})
AD_TOKENS = frozenset({"ad", "ads", "adv", "advert", "adverts", "advertisement", "advertising", "adsense", "adslot",
                       "adunit", "adbox", "adblock", "adcontainer", "adwrapper", "dfp", "doubleclick", "sponsor",
                       "sponsored", "sponsors", "promoted", "taboola", "outbrain", "affiliate"})
AD_PREFIXES = ("advert", "sponsor", "adsense", "adslot", "adunit", "googlead", "doubleclick", "taboola", "outbrain")
CONTENT_TOKENS = frozenset({"content", "maincontent", "article", "articlebody", "post", "postcontent", "entry",
                            "entrycontent", "story", "storybody", "body", "text", "prose", "markdown", "document"})
CHROME_PHRASES = re.compile(
    r"^(skip to (main )?content|skip navigation|jump to (navigation|search)|toggle navigation|menu|main menu"
    r"|close|open menu|search( this site)?|search for:?|go|submit|log ?in|sign ?in|sign ?up|register|log ?out"
    r"|my account|home|back to top|top|print( this page)?|share( this( page| article)?)?( on \w+)?|tweet|pin it"
    r"|follow us( on \w+)?|like us on facebook|email this|copy link|read more|continue reading|click here"
    r"|next|previous|prev|older posts|newer posts|load more|show more|see more|more|page \d+( of \d+)?"
    r"|you are here:?|breadcrumbs?|loading\.*|please wait\.*|advertisement|advertisements|sponsored"
    r"|sponsored content|ad|ads by \w+|paid content|promoted|accept( all)?( cookies)?|reject all|manage cookies"
    r"|cookie settings|got it|ok|i agree|we use cookies.{0,200}|this (web)?site uses cookies.{0,200}"
    r"|by continuing to (use|browse).{0,200}|subscribe( to our newsletter)?|sign up for our newsletter.{0,120}"
    r"|enter your email( address)?|all rights reserved\.?|(copyright )?(©|\(c\)) ?\d{4}.{0,120}"
    r"|privacy policy|terms of (use|service)|cookie policy|contact us|about us|sitemap)[.!:]?$", re.I)
AD_PHRASES = re.compile(r"^(advertisement|advertisements|sponsored( content)?|ad|ads|paid (content|post)|promoted"
                        r"|partner content|affiliate disclosure.{0,200})[.:]?$", re.I)
_SPACE = re.compile(r"[ \t\r\n\f\v  -​    　]+")
_CAMEL = re.compile(r"([a-z0-9])([A-Z])")
_TOKEN_SPLIT = re.compile(r"[^a-z0-9]+")
_STYLE_HIDDEN = re.compile(r"(display\s*:\s*none|visibility\s*:\s*hidden)", re.I)


class HtmlExtractionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class HtmlDeadline(RuntimeError):
    """The wall-clock safety net fired. Not a deterministic outcome: the caller retries later."""


@dataclass
class Node:
    tag: str
    attrs: dict[str, str]
    parent: "Node | None"
    children: list["Node | str"] = field(default_factory=list)
    removed: str | None = None
    text_len: int = 0
    link_len: int = 0
    links: int = 0
    has_h1: bool = False

    def iter(self):
        stack: list[Node] = [self]
        while stack:
            node = stack.pop()
            yield node
            stack.extend(c for c in reversed(node.children) if isinstance(c, Node))


class ListLine(str):
    """A list item: consecutive items are joined by single newlines (one list block)."""


@dataclass
class HtmlResult:
    text: str
    title: str | None
    removed: Counter
    stats: Counter


# --------------------------------------------------------------------------- #
# 05 parse
# --------------------------------------------------------------------------- #

class _Builder(HTMLParser):
    def __init__(self, max_nodes: int, max_depth: int, tick: Callable[[], None]) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Node("#root", {}, None)
        self.stack: list[Node] = [self.root]
        self.max_nodes, self.max_depth, self.tick = max_nodes, max_depth, tick
        self.nodes = 0
        self.title: list[str] | None = None
        self.title_done = False

    def _close_to(self, names: frozenset[str] | set[str], boundary: frozenset[str] | set[str]) -> None:
        for i in range(len(self.stack) - 1, 0, -1):
            tag = self.stack[i].tag
            if tag in names:
                del self.stack[i:]
                return
            if tag in boundary:
                return

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.nodes += 1
        if self.nodes > self.max_nodes:
            raise HtmlExtractionError("html_too_many_nodes", f"more than {self.max_nodes} HTML elements")
        if self.nodes % 2000 == 0:
            self.tick()
        if tag in BLOCK or tag in ("li", "dt", "dd", "tr", "td", "th"):
            if tag in ("li",):
                self._close_to({"li"}, {"ul", "ol", "menu", "table"})
            elif tag in ("dt", "dd"):
                self._close_to({"dt", "dd"}, {"dl", "table"})
            elif tag == "tr":
                self._close_to({"tr"}, {"table", "tbody", "thead", "tfoot"})
            elif tag in ("td", "th"):
                self._close_to({"td", "th"}, {"tr", "table"})
            elif tag in ("tbody", "thead", "tfoot"):
                self._close_to({"tbody", "thead", "tfoot"}, {"table"})
            if tag not in ("td", "th", "tr", "li", "dt", "dd"):
                self._close_to({"p"}, BLOCK - {"p"})
        elif tag == "option":
            self._close_to({"option"}, {"select", "datalist"})
        node = Node(tag, {k.lower(): (v or "") for k, v in attrs[:64]}, self.stack[-1])
        self.stack[-1].children.append(node)
        if tag == "title" and self.title is None:
            self.title = []
        if tag not in VOID:
            self.stack.append(node)
            if len(self.stack) > self.max_depth:
                raise HtmlExtractionError("html_too_deep", f"HTML nesting deeper than {self.max_depth}")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in VOID and self.stack[-1].tag == tag:
            self.stack.pop()

    def handle_endtag(self, tag: str) -> None:
        if tag == "title" and self.title is not None:
            self.title_done = True
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data: str) -> None:
        parent = self.stack[-1]
        if parent.tag == "title" and self.title is not None and not self.title_done:
            self.title.append(data)
        if parent.children and isinstance(parent.children[-1], str):
            parent.children[-1] += data
        else:
            parent.children.append(data)


def parse_html(text: str, *, max_nodes: int, max_depth: int,
               tick: Callable[[], None] = lambda: None) -> tuple[Node, str | None]:
    builder = _Builder(max_nodes, max_depth, tick)
    try:
        builder.feed(text)
        builder.close()
    except HtmlExtractionError:
        raise
    except (AssertionError, ValueError, IndexError) as exc:  # html.parser on pathological input
        raise HtmlExtractionError("html_unparseable", f"HTML could not be parsed: {type(exc).__name__}") from None
    title = None
    if builder.title:
        title = " ".join("".join(builder.title).split()) or None
    return builder.root, title


# --------------------------------------------------------------------------- #
# 06-09 removal
# --------------------------------------------------------------------------- #

def _tokens(node: Node) -> set[str]:
    raw = f"{node.attrs.get('class', '')} {node.attrs.get('id', '')}"
    if not raw.strip():
        return set()
    tokens = set(_TOKEN_SPLIT.split(_CAMEL.sub(r"\1 \2", raw).lower())) - {""}
    joined = {t for t in _TOKEN_SPLIT.split(raw.lower()) if t}  # e.g. "sr-only" -> {"sr", "only"}
    compact = {re.sub(r"[^a-z0-9]", "", part) for part in raw.lower().split()}
    return tokens | joined | compact


def _is_hidden(node: Node) -> bool:
    attrs = node.attrs
    if "hidden" in attrs or attrs.get("aria-hidden", "").strip().lower() == "true":
        return True
    if attrs.get("type", "").lower() == "hidden":
        return True
    if _STYLE_HIDDEN.search(attrs.get("style", "")):
        return True
    classes = attrs.get("class", "").lower().split()
    if any(c in HIDDEN_CLASS_NAMES for c in classes):
        return True
    # "d-none" alone hides; "d-none d-md-block" is responsive content, not hidden.
    return "d-none" in classes and not any(c.startswith("d-") and c != "d-none" for c in classes)


def _measure(root: Node) -> None:
    """Post-order text and link-text lengths of kept content (iterative)."""
    order = [n for n in root.iter()]
    for node in reversed(order):
        text = link = 0
        links = 1 if node.tag == "a" else 0
        has_h1 = node.tag == "h1"
        if node.removed is None:
            for child in node.children:
                if isinstance(child, str):
                    size = len(child.strip())
                    text += size
                    if node.tag == "a":
                        link += size
                elif child.removed is None:
                    text += child.text_len
                    link += child.text_len if node.tag == "a" else child.link_len
                    links += child.links
                    has_h1 = has_h1 or child.has_h1
        node.text_len, node.link_len, node.links, node.has_h1 = text, link, links, has_h1


def _inside(node: Node, tags: frozenset[str]) -> bool:
    parent = node.parent
    while parent is not None:
        if parent.tag in tags or parent.attrs.get("role", "").lower() in ("main", "article"):
            return True
        parent = parent.parent
    return False


def _mark(root: Node, removed: Counter, page_text: int) -> None:
    for node in root.iter():
        if node.removed is not None or node.tag == "#root":
            continue
        if _removed_ancestor(node):
            continue
        tokens = _tokens(node)
        tag, role = node.tag, node.attrs.get("role", "").strip().lower()
        reason = None
        if tag in ACTIVE:
            reason = f"active_{tag}"
        elif _is_hidden(node):
            reason = "hidden"
        elif tag in SEMANTIC_CHROME or role in CHROME_ROLES:
            reason = f"boilerplate_{tag if tag in SEMANTIC_CHROME else 'role_' + role}"
        elif tag in ("header", "footer") and not _inside(node, frozenset({"article", "main"})):
            reason = f"boilerplate_site_{tag}"
        elif tag == "form" and node.text_len < 400:
            reason = "chrome_form"
        elif tag not in ("html", "body", "main", "article") and role not in ("main", "article") and tokens:
            if tokens & AD_TOKENS or any(t.startswith(AD_PREFIXES) for t in tokens):
                reason = "ads_container"
            elif (tokens & CHROME_TOKENS) and not (tokens & CONTENT_TOKENS and node.text_len > page_text * 0.3):
                if not (tokens & HEADER_LIKE and node.has_h1):
                    reason = "chrome_" + sorted(tokens & CHROME_TOKENS)[0]
            if reason is not None and page_text and node.text_len > page_text * 0.5:
                reason = None  # holds most of the page text: a mis-named wrapper, not chrome
        if reason is not None:
            node.removed = reason
            removed[reason] += 1


def _removed_ancestor(node: Node) -> bool:
    parent = node.parent
    while parent is not None:
        if parent.removed is not None:
            return True
        parent = parent.parent
    return False


def _prune_link_dense(root: Node, removed: Counter) -> None:
    for node in root.iter():
        if node.removed is not None or node is root or node.tag not in BLOCK or node.tag in HEADINGS:
            continue
        if node.text_len == 0 or _removed_ancestor(node):
            continue
        links = node.links
        density = node.link_len / node.text_len
        plain_words = (node.text_len - node.link_len) / 6
        if (density >= 0.6 and links >= 3) or (links >= 8 and density >= 0.4 and plain_words < 40):
            node.removed = "chrome_link_dense"
            removed["chrome_link_dense"] += 1


def _select_main(body: Node) -> Node:
    total = body.text_len or 1
    candidates: list[tuple[int, int, Node]] = []
    for rank, node in enumerate(body.iter()):
        if node.removed is not None or node is body or _removed_ancestor(node):
            continue
        role = node.attrs.get("role", "").lower()
        if node.tag == "main" or role == "main":
            candidates.append((0, rank, node))
        elif node.tag == "article" or role == "article":
            candidates.append((1, rank, node))
        elif _tokens(node) & {"maincontent", "articlebody", "postcontent", "entrycontent", "storybody", "content"}:
            candidates.append((2, rank, node))
    for priority in (0, 1, 2):
        group = [n for p, _r, n in candidates if p == priority and n.text_len >= total * 0.3]
        if group:
            return max(group, key=lambda n: n.text_len)
    return body


# --------------------------------------------------------------------------- #
# 10 structure reconstruction
# --------------------------------------------------------------------------- #

def _inline(node: Node | str, out: list[str], stats: Counter) -> None:
    if isinstance(node, str):
        out.append(node)
        return
    if node.removed is not None:
        return
    if node.tag == "br":
        out.append("\n")
        return
    for child in node.children:
        _inline(child, out, stats)


def _clean_inline(parts: list[str]) -> str:
    lines = ["".join(_SPACE.sub(" ", seg)).strip() for seg in "".join(parts).split("\n")]
    return "\n".join(line for line in lines if line)


def _has_block_child(node: Node) -> bool:
    return any(isinstance(c, Node) and c.removed is None and (c.tag in BLOCK or c.tag in ("li", "pre", "ul", "ol"))
               for c in node.children)


def _table(node: Node, blocks: list[str], stats: Counter, emit: Callable[[Node, int], None]) -> None:
    rows: list[list[str]] = []
    nested = False
    for row in node.iter():
        if row.tag == "table" and row is not node:
            nested = True
        if row.tag == "tr" and row.removed is None:
            cells = []
            for cell in row.children:
                if isinstance(cell, Node) and cell.tag in ("td", "th") and cell.removed is None:
                    parts: list[str] = []
                    _inline(cell, parts, stats)
                    cells.append(" ".join(_clean_inline(parts).split("\n")))
            if any(cells):
                rows.append(cells)
    width = max((len(r) for r in rows), default=0)
    long_cells = sum(1 for r in rows for c in r if len(c) > 300)
    if nested or width < 2 or len(rows) < 2 or long_cells:
        stats["layout_tables"] += 1
        for child in node.children:  # a layout table: its cells are ordinary blocks
            if isinstance(child, Node):
                emit(child, 0)
        return
    stats["tables"] += 1
    caption = next((c for c in node.children if isinstance(c, Node) and c.tag == "caption" and c.removed is None), None)
    if caption is not None:
        parts: list[str] = []
        _inline(caption, parts, stats)
        if _clean_inline(parts):
            blocks.append(_clean_inline(parts))
    blocks.append("\n".join(" | ".join(cells) for cells in rows))


def _emit_blocks(root: Node, stats: Counter, tick: Callable[[], None]) -> list[str]:
    blocks: list[str] = []
    counter = [0]

    def emit(node: Node, list_depth: int) -> None:
        counter[0] += 1
        if counter[0] % 2000 == 0:
            tick()
        if node.removed is not None:
            return
        tag = node.tag
        if tag in HEADINGS:
            parts: list[str] = []
            _inline(node, parts, stats)
            text = " ".join(_clean_inline(parts).split("\n"))
            if text:
                blocks.append("#" * HEADINGS[tag] + " " + text)
                stats["headings"] += 1
            return
        if tag == "pre":
            raw = []
            _inline(node, raw, stats)
            code = "\n".join(line.rstrip() for line in "".join(raw).strip("\n").split("\n"))
            if code.strip():
                blocks.append("```\n" + code + "\n```")
                stats["pre_blocks"] += 1
            return
        if tag in ("ul", "ol", "menu"):
            number = 0
            for child in node.children:
                if isinstance(child, Node) and child.tag == "li" and child.removed is None:
                    number += 1
                    parts = []
                    nested: list[Node] = []
                    for grand in child.children:
                        if isinstance(grand, Node) and grand.tag in ("ul", "ol") and grand.removed is None:
                            nested.append(grand)
                        else:
                            _inline(grand, parts, stats)
                    text = " ".join(_clean_inline(parts).split("\n"))
                    if text:
                        marker = f"{number}." if tag == "ol" else "-"
                        blocks.append(ListLine("  " * list_depth + f"{marker} {text}"))
                        stats["list_items"] += 1
                    for sub in nested:
                        emit(sub, list_depth + 1)
                elif isinstance(child, Node):
                    emit(child, list_depth)
            return
        if tag == "table":
            _table(node, blocks, stats, emit)
            return
        if tag == "dl":
            term = None
            for child in node.children:
                if isinstance(child, Node) and child.removed is None and child.tag in ("dt", "dd"):
                    parts = []
                    _inline(child, parts, stats)
                    text = " ".join(_clean_inline(parts).split("\n"))
                    if child.tag == "dt":
                        term = text
                    elif text:
                        blocks.append(f"{term}: {text}" if term else text)
                        stats["definitions"] += 1
                        term = None
            return
        if tag == "blockquote" and not _has_block_child(node):
            parts = []
            _inline(node, parts, stats)
            text = _clean_inline(parts)
            if text:
                blocks.append("\n".join("> " + line for line in text.split("\n")))
            return
        if tag in BLOCK or tag in ("li", "figure", "figcaption", "#root") or _has_block_child(node):
            buffer: list[str] = []

            def flush() -> None:
                text = _clean_inline(buffer)
                buffer.clear()
                if text:
                    blocks.append(text)
                    stats["paragraphs"] += 1

            for child in node.children:
                if isinstance(child, str):
                    buffer.append(child)
                elif child.removed is not None:
                    continue
                elif child.tag in BLOCK or child.tag in HEADINGS or child.tag in ("pre", "ul", "ol", "li", "table",
                                                                                  "dl", "figure", "figcaption") \
                        or _has_block_child(child):
                    flush()
                    emit(child, list_depth)
                else:
                    _inline(child, buffer, stats)
            flush()
            return
        parts = []
        _inline(node, parts, stats)
        text = _clean_inline(parts)
        if text:
            blocks.append(text)

    emit(root, 0)
    return blocks


def _filter_blocks(blocks: list[str], removed: Counter) -> list[str]:
    counts = Counter(b for b in blocks if len(b.split()) <= 30)
    kept: list[str] = []
    seen: set[str] = set()
    for block in blocks:
        words = len(block.split())
        plain = block.lstrip("#-> 0123456789.").strip()
        if words <= 20 and AD_PHRASES.match(plain):
            removed["ads_label"] += 1
            continue
        if words <= 40 and CHROME_PHRASES.match(plain):
            removed["chrome_phrase"] += 1
            continue
        if counts.get(block, 0) >= 3:
            removed["chrome_repeated_template"] += 1
            continue
        if block in seen and words >= 3 and not block.startswith("#"):
            removed["duplicate_block"] += 1
            continue
        seen.add(block)
        kept.append(block)
    return kept


def join_blocks(blocks: list[str]) -> str:
    out: list[str] = []
    for block in blocks:
        if out and isinstance(block, ListLine) and isinstance(out[-1], ListLine):
            out[-1] = ListLine(out[-1] + "\n" + block)
        else:
            out.append(block)
    return "\n\n".join(out)


def extract_html(text: str, *, max_nodes: int, max_depth: int, max_chars: int, deadline_seconds: float,
                 clock: Callable[[], float] = time.monotonic) -> HtmlResult:
    """Parse and clean one HTML document into structured text. Raises HtmlExtractionError / HtmlDeadline."""
    start = clock()

    def tick() -> None:
        if clock() - start > deadline_seconds:
            raise HtmlDeadline(f"HTML extraction exceeded {deadline_seconds}s")

    root, title = parse_html(text, max_nodes=max_nodes, max_depth=max_depth, tick=tick)
    removed: Counter = Counter()
    stats: Counter = Counter()
    body = next((n for n in root.iter() if n.tag == "body"), root)
    _measure(root)
    stats["raw_text_chars"] = body.text_len
    _mark(root, removed, body.text_len)
    _measure(root)
    main = _select_main(body)
    stats["main_selected"] = int(main is not body)
    _prune_link_dense(main, removed)
    _measure(root)
    blocks: list[str] = []
    if main is not body and not main.has_h1:
        h1s = [n for n in body.iter() if n.tag == "h1" and n.removed is None and not _removed_ancestor(n)]
        if len(h1s) == 1:
            blocks.extend(_emit_blocks(h1s[0], stats, tick))
    blocks.extend(_emit_blocks(main, stats, tick))
    tick()
    blocks = _filter_blocks(blocks, removed)
    output = join_blocks(blocks)
    if len(output) > max_chars:
        raise HtmlExtractionError("extracted_too_large", f"extracted text exceeds {max_chars} characters")
    stats["kept_chars"] = len(output)
    stats["link_chars"] = main.link_len
    return HtmlResult(output, html_lib.unescape(title) if title else None, removed, stats)


def extract_plain_text(text: str, *, max_chars: int) -> str:
    """text/plain payloads: paragraphs as recorded (no structure is invented)."""
    if len(text) > max_chars:
        raise HtmlExtractionError("extracted_too_large", f"extracted text exceeds {max_chars} characters")
    return text
