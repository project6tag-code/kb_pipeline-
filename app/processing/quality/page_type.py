"""Page-type and synthetic-content checks for web pages (PAGE_TYPE_VERSION ``ptype1``).

Part of gate 16 (noise): any hit fails the gate, so the document is rejected
(``rejected_quality``). They answer "is this page an article-like factual text
written by its publisher?". Commerce, account, legal-boilerplate, listing,
forum and synthetic / placeholder pages are not, whatever their wording.
Applies to Common Crawl pages only; code files have their own rules.

| reason                 | rule                                                                                  |
|------------------------|---------------------------------------------------------------------------------------|
| non_content_url:<why>  | the URL fails URL policy ``url1`` (``app.sources.commoncrawl.index.url_rejection``)   |
| non_content_title      | the title names a shop, account, legal, search, archive/listing or error page         |
| schema_type:<Type>     | the page declares (JSON-LD ``@type``) a product, offer, shop, local business, job,    |
|                        | event, search-results, checkout, profile, forum or Q&A page                           |
| commerce_page          | >= 3 shopping phrases (add to cart, in stock, SKU ...) or >= 6 prices per 1,000 words |
| listing_page           | >= 4 "read more" / "continue reading" teasers                                          |
| forum_or_comments      | >= 6 forum / comment markers (posted by, reply, joined:, quote ...) per 1,000 words  |
| synthetic_or_placeholder | AI-assistant boilerplate, AI-generation notices, lorem ipsum / dummy / sample text, |
|                        | unfilled template slots (``[insert ...]``, ``{{name}}``, ``%%var%%``)                  |
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.processing.quality.scorer import TextFeatures
from app.sources.commoncrawl.dates import jsonld_nodes
from app.sources.commoncrawl.index import url_rejection

VERSION = "ptype1"

# Site-chrome phrasing only: topic words such as "Register", "Deal" or "Archives" alone are real
# article titles ("National Register of Historic Places", "New Deal", "National Archives").
_TITLE = re.compile(
    r"(\bshopping cart\b|\bonline store\b|\badd to cart\b|\bcheckout\b|\bbuy (now|online)\b|\bbest price\b"
    r"|\b(daily|hot|today'?s) deals\b|\bcoupons?\b|\bpromo codes?\b|\b(log|sign) (in|up)( to| for|$)"
    r"|^(log ?in|sign ?in|sign ?up|register|my account)\b|\bcreate an account\b|\bsearch results?\b"
    r"|\bresults for\b|\bterms (of|and) (use|service|conditions)\b|\bprivacy (policy|notice)\b"
    r"|\bcookie (policy|settings)\b|^disclaimer\b|\bcontact us\b|\bcareers at\b|\bjob openings\b"
    r"|\b(monthly|tag|category|author) archives?\b|\barchives? for\b|\bpage \d+ of \d+\b"
    r"|\b(page|file) not found\b|\berror \d{3}\b|\baccess denied\b|^index of /)", re.I)
# Marketplace phrasing only. Words that are also technical vocabulary ("git checkout", an API doc's
# "Returns:", "Shipping:") are deliberately absent: a real Stack Overflow git page matched them.
_SHOPPING = re.compile(r"(add to (cart|basket|bag)|in stock|out of stock|\bsku\b|free shipping|quantity:"
                       r"|item #|model #|buy (it )?now|order now|add to (wish ?list|watch ?list)|proceed to checkout"
                       r"|shipping & returns|free returns|item specifics|have one to sell|top rated seller"
                       r"|money back guarantee|people are watching|\d[\d,]* sold\b)", re.I)
# Publisher-declared schema.org types (JSON-LD ``@type``) of pages that are not factual KB material.
NON_CONTENT_SCHEMA_TYPES = frozenset({
    "Product", "ProductGroup", "IndividualProduct", "ProductModel", "Offer", "AggregateOffer", "Store", "OnlineStore",
    "LocalBusiness", "Restaurant", "Hotel", "LodgingBusiness", "RealEstateListing", "JobPosting", "Event",
    "SearchResultsPage", "CheckoutPage", "ProfilePage", "DiscussionForumPosting", "QAPage", "SocialMediaPosting",
})
_PRICE = re.compile(r"(?:[$£€¥]\s?\d[\d,]*(?:\.\d{2})?|\b\d[\d,]*\.\d{2}\s?(?:usd|eur|gbp)\b)", re.I)
_TEASER = re.compile(r"\b(read more|continue reading|read the full (story|article)|more\s*»)", re.I)
_FORUM = re.compile(r"\b(posted by|reply|replies|quote|joined:|posts:|thread|re:|reputation:|member since"
                    r"|permalink|upvote|report post|log in to reply)\b", re.I)
_SYNTHETIC = re.compile(
    r"(as an ai (language )?model|i(?: am|'m) an ai\b|i(?: am|'m) a large language model"
    r"|(generated|written|created) (by|with|using) (an? )?(ai|artificial intelligence|chatgpt|gpt-?\d|openai|llm)"
    r"|this (article|content|text|page|post) (was|has been|is) (ai[- ])?(generated|written) (by|with|using)"
    r"|lorem ipsum|dolor sit amet|dummy (text|content)|sample (text|content|page|post)|placeholder (text|content)"
    r"|this is (an?|the) (example|sample|test|demo) (page|post|article|site)|hello world!? welcome to wordpress"
    r"|\[insert [^\]]{1,60}\]|\{\{\s*[a-z_][\w.]*\s*\}\}|%%[a-z_]\w*%%|\$\{[a-z_]\w*\})", re.I)


@dataclass(frozen=True)
class PageTypeResult:
    passed: bool
    reasons: tuple[str, ...]


def schema_types(html: str | None) -> frozenset[str]:
    """The ``@type`` values a page declares in JSON-LD (bounded; malformed blocks are skipped)."""
    if not html or "ld+json" not in html:
        return frozenset()
    found: set[str] = set()
    for node in jsonld_nodes(html):
        value = node.get("@type")
        for item in value if isinstance(value, list) else [value]:
            if isinstance(item, str) and len(item) <= 80:
                found.add(item.rsplit("/", 1)[-1])
    return frozenset(found)


def check(url: str | None, title: str | None, text: str, f: TextFeatures,
          declared_types: frozenset[str] = frozenset()) -> PageTypeResult:
    reasons: list[str] = []
    if url:
        why = url_rejection(url)
        if why is not None:
            reasons.append(f"non_content_url:{why}")
    if title and _TITLE.search(title):
        reasons.append("non_content_title")
    for declared in sorted(declared_types & NON_CONTENT_SCHEMA_TYPES):
        reasons.append(f"schema_type:{declared}")
    per_k = 1000.0 / max(1, f.words)
    if len(_SHOPPING.findall(text)) >= 3 or len(_PRICE.findall(text)) * per_k >= 6:
        reasons.append("commerce_page")
    if len(_TEASER.findall(text)) >= 4:
        reasons.append("listing_page")
    if len(_FORUM.findall(text)) * per_k >= 6 and len(_FORUM.findall(text)) >= 5:
        reasons.append("forum_or_comments")
    if _SYNTHETIC.search(text) or (title and _SYNTHETIC.search(title)):
        reasons.append("synthetic_or_placeholder")
    return PageTypeResult(not reasons, tuple(reasons))
