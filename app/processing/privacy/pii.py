"""Gate 14: PII / privacy analysis (PRIVACY_DETECTOR_VERSION ``pii1``).

Context-aware and conservative. Ordinary person names and role/business
contact addresses are NOT removed. Every detector validates its match
(Luhn, IBAN mod-97, Verhoeff, SSN ranges) before it counts.

| Category            | Default action                                              |
|---------------------|-------------------------------------------------------------|
| personal_email      | REDACT (role/business local parts and example domains: ALLOW) |
| phone               | REDACT (formatted numbers, or digits next to a phone keyword) |
| card_number         | REJECT (Luhn-valid, known issuer); published test cards: ALLOW |
| ssn                 | REJECT with an SSN keyword nearby, else REDACT              |
| national_id         | Aadhaar (Verhoeff + keyword): REJECT; UK NINO / passport with keyword: REDACT |
| bank_account        | IBAN (mod-97 valid): REDACT; documented example IBANs: ALLOW |
| street_address      | ALLOW (counted); REDACT inside a personal contact block     |
| ip_address          | ALLOW (counted; technical content)                          |

Decision: any REJECT finding -> REJECT; at least ``quarantine_count``
personal identifiers -> QUARANTINE (a contact dump or leak); any REDACT
finding -> REDACT (deterministic ``[REDACTED_<CATEGORY>]`` tokens); else ALLOW.
Results carry categories and counts only, never the matched values.
"""

from __future__ import annotations

import ipaddress
import re
from collections import Counter
from dataclasses import dataclass, field

VERSION = "pii1"

ALLOW, REDACT, REJECT, QUARANTINE = "ALLOW", "REDACT", "REJECT", "QUARANTINE"

_EMAIL = re.compile(r"(?<![\w.+%-])([A-Za-z0-9][A-Za-z0-9._%+-]{0,63})@"
                    r"((?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,24})(?![\w-])")
_ROLE_LOCALS = frozenset({
    "info", "contact", "support", "help", "helpdesk", "hello", "hi", "team", "sales", "admin", "administrator",
    "office", "press", "media", "news", "marketing", "billing", "accounts", "finance", "hr", "jobs", "careers",
    "recruiting", "legal", "privacy", "security", "abuse", "postmaster", "hostmaster", "webmaster", "noreply",
    "no-reply", "donotreply", "do-not-reply", "enquiries", "inquiries", "feedback", "service", "customerservice",
    "orders", "shop", "store", "partners", "partnerships", "events", "editor", "editors", "editorial", "community",
    "dev", "developers", "devnull", "root", "mail", "general", "reception", "booking", "bookings", "reservations",
    "membership", "members", "registrar", "library", "research", "communications", "comms", "ops", "operations",
    "it", "tech", "technical", "licensing", "opensource", "oss", "maintainers", "security-team", "bugs", "issues",
})
_BENIGN_DOMAINS = re.compile(r"(^|\.)(example\.(com|org|net|edu)|[a-z0-9-]+\.(example|test|invalid|localhost)"
                             r"|users\.noreply\.github\.com|noreply\.github\.com|(your|my)?domain\.com|email\.com"
                             r"|company\.com|acme\.com|foo\.(com|bar)|bar\.com|mail\.com|test\.com)$", re.I)
_PHONE = re.compile(r"(?<![\w+/.-])(?:\+\d{1,3}[\s.-]?)?(?:\(\d{1,4}\)[\s.-]?)?\d{2,4}(?:[\s.-]\d{2,4}){1,4}(?![\w/.-])")
_PHONE_CONTEXT = re.compile(r"(phone|tel\b|telephone|call\b|mobile|cell\b|fax|whatsapp|contact|ph\.|téléphone)",
                            re.I)
_NANP = re.compile(r"^(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$")
_SSN = re.compile(r"(?<![\d-])(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}(?![\d-])")
_SSN_CONTEXT = re.compile(r"(ssn|social security|soc\.? sec)", re.I)
_CARD = re.compile(r"(?<![\d-])(?:\d[ -]?){12,18}\d(?![\d-])")
_TEST_CARDS = frozenset({"4111111111111111", "4242424242424242", "4012888888881881", "4000056655665556",
                         "5555555555554444", "5105105105105100", "2223003122003222", "378282246310005",
                         "371449635398431", "6011111111111117", "6011000990139424", "3056930009020004",
                         "36227206271667", "3566002020360505", "6200000000000005", "4000000000000002",
                         "4000000000009995", "4000000000000077", "5200828282828210", "4222222222222"})
_IBAN = re.compile(r"(?<![A-Z0-9])([A-Z]{2}\d{2}(?: ?[A-Z0-9]{4}){2,7}(?: ?[A-Z0-9]{1,4})?)(?![A-Z0-9])")
_EXAMPLE_IBANS = frozenset({"DE89370400440532013000", "GB82WEST12345698765432", "GB29NWBK60161331926819",
                            "FR1420041010050500013M02606", "NL91ABNA0417164300", "CH9300762011623852957"})
_AADHAAR = re.compile(r"(?<!\d)[2-9]\d{3} ?\d{4} ?\d{4}(?!\d)")
_AADHAAR_CONTEXT = re.compile(r"(aadhaar|aadhar|uidai)", re.I)
_NINO = re.compile(r"(?<![A-Z0-9])(?!BG|GB|NK|KN|TN|NT|ZZ)[A-CEGHJ-PR-TW-Z][A-CEGHJ-NPR-TW-Z] ?\d{2} ?\d{2} ?\d{2} ?[A-D]"
                   r"(?![A-Z0-9])")
_NINO_CONTEXT = re.compile(r"(national insurance|\bnino\b|\bni number)", re.I)
_PASSPORT = re.compile(r"passport\s*(?:no\.?|number|#)\s*[:#]?\s*([A-Z0-9]{6,9})\b", re.I)
_STREET = re.compile(r"\b\d{1,5}\s+(?:[A-Z][a-z]+\.?\s){1,3}(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln"
                     r"|Drive|Dr|Court|Ct|Place|Pl|Way|Terrace|Parkway|Pkwy|Highway|Hwy)\b\.?")
_IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")

_VERHOEFF_D = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 2, 3, 4, 0, 6, 7, 8, 9, 5], [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
               [3, 4, 0, 1, 2, 8, 9, 5, 6, 7], [4, 0, 1, 2, 3, 9, 5, 6, 7, 8], [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
               [6, 5, 9, 8, 7, 1, 0, 4, 3, 2], [7, 6, 5, 9, 8, 2, 1, 0, 4, 3], [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
               [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]]
_VERHOEFF_P = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 5, 7, 6, 2, 8, 3, 0, 9, 4], [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
               [8, 9, 1, 6, 0, 4, 3, 5, 2, 7], [9, 4, 5, 3, 1, 2, 6, 8, 7, 0], [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
               [2, 7, 9, 3, 8, 0, 6, 4, 1, 5], [7, 0, 4, 6, 9, 1, 3, 2, 5, 8]]


@dataclass(frozen=True)
class Finding:
    category: str
    start: int
    end: int
    action: str          # ALLOW | REDACT | REJECT
    reason: str


@dataclass(frozen=True)
class PiiResult:
    decision: str
    text: str                                  # redacted text when REDACT, else the input
    categories: dict[str, int] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()
    redactions: int = 0


def luhn_valid(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 1:
            n = n * 2 - 9 if n > 4 else n * 2
        total += n
    return total % 10 == 0


def _issuer(digits: str) -> bool:
    n = len(digits)
    if digits[0] == "4":
        return n in (13, 16, 19)
    two, four = int(digits[:2]), int(digits[:4])
    if 51 <= two <= 55 or 2221 <= four <= 2720:
        return n == 16
    if two in (34, 37):
        return n == 15
    if digits.startswith(("6011", "65")) or 644 <= int(digits[:3]) <= 649:
        return 16 <= n <= 19
    if 3528 <= four <= 3589:
        return 16 <= n <= 19
    if two in (36, 38) or 300 <= int(digits[:3]) <= 305:
        return 14 <= n <= 19
    return False


def iban_valid(value: str) -> bool:
    raw = value.replace(" ", "")
    if not 15 <= len(raw) <= 34:
        return False
    rearranged = raw[4:] + raw[:4]
    number = "".join(str(int(ch, 36)) for ch in rearranged)
    return int(number) % 97 == 1


def verhoeff_valid(digits: str) -> bool:
    check = 0
    for i, ch in enumerate(reversed(digits)):
        check = _VERHOEFF_D[check][_VERHOEFF_P[i % 8][int(ch)]]
    return check == 0


def _near(pattern: re.Pattern[str], text: str, start: int, end: int, window: int = 40) -> bool:
    return pattern.search(text, max(0, start - window), min(len(text), end + window)) is not None


def _findings(text: str) -> list[Finding]:
    found: list[Finding] = []
    for m in _CARD.finditer(text):
        digits = re.sub(r"\D", "", m.group(0))
        if 13 <= len(digits) <= 19 and luhn_valid(digits) and _issuer(digits) and len(set(digits)) > 2:
            action = ALLOW if digits in _TEST_CARDS else REJECT
            found.append(Finding("card_number", m.start(), m.end(), action,
                                 "test_card" if action == ALLOW else "luhn_valid_card"))
    for m in _IBAN.finditer(text):
        raw = m.group(1).replace(" ", "")
        if iban_valid(raw):
            action = ALLOW if raw in _EXAMPLE_IBANS else REDACT
            found.append(Finding("bank_account", m.start(1), m.end(1), action,
                                 "example_iban" if action == ALLOW else "iban"))
    for m in _SSN.finditer(text):
        context = _near(_SSN_CONTEXT, text, m.start(), m.end())
        found.append(Finding("ssn", m.start(), m.end(), REJECT if context else REDACT,
                             "ssn_with_context" if context else "ssn_pattern"))
    for m in _AADHAAR.finditer(text):
        digits = m.group(0).replace(" ", "")
        if verhoeff_valid(digits) and _near(_AADHAAR_CONTEXT, text, m.start(), m.end(), 60):
            found.append(Finding("national_id", m.start(), m.end(), REJECT, "aadhaar"))
    for m in _NINO.finditer(text):
        if _near(_NINO_CONTEXT, text, m.start(), m.end(), 60):
            found.append(Finding("national_id", m.start(), m.end(), REDACT, "uk_nino"))
    for m in _PASSPORT.finditer(text):
        found.append(Finding("national_id", m.start(1), m.end(1), REDACT, "passport_number"))
    for m in _EMAIL.finditer(text):
        local, domain = m.group(1).lower(), m.group(2).lower()
        base_local = re.split(r"[+]", local)[0]
        if _BENIGN_DOMAINS.search(domain):
            found.append(Finding("email", m.start(), m.end(), ALLOW, "example_email"))
        elif base_local in _ROLE_LOCALS or base_local.split(".")[0] in _ROLE_LOCALS:
            found.append(Finding("email", m.start(), m.end(), ALLOW, "role_email"))
        else:
            found.append(Finding("personal_email", m.start(), m.end(), REDACT, "personal_email"))
    for m in _PHONE.finditer(text):
        value = m.group(0)
        digits = re.sub(r"\D", "", value)
        if not 10 <= len(digits) <= 15 or len(set(digits)) <= 2:
            continue
        formatted = bool(_NANP.match(value.strip())) or value.startswith("+") or "(" in value
        if formatted or _near(_PHONE_CONTEXT, text, m.start(), m.end(), 30):
            found.append(Finding("phone", m.start(), m.end(), REDACT, "phone_number"))
    for m in _STREET.finditer(text):
        found.append(Finding("street_address", m.start(), m.end(), ALLOW, "street_address"))
    for m in _IPV4.finditer(text):
        try:
            ip = ipaddress.IPv4Address(m.group(0))
        except ValueError:
            continue
        if ip.is_global:
            found.append(Finding("ip_address", m.start(), m.end(), ALLOW, "public_ip"))
    # Earlier (stronger) detectors win on overlapping spans.
    kept: list[Finding] = []
    for finding in found:
        if not any(f.start < finding.end and finding.start < f.end for f in kept):
            kept.append(finding)
    # A street address inside a personal contact block is personal contact data.
    personal = [f for f in kept if f.category in ("personal_email", "phone")]
    result = []
    for f in kept:
        if f.category == "street_address" and any(abs(p.start - f.start) <= 200 for p in personal):
            f = Finding(f.category, f.start, f.end, REDACT, "personal_contact_block")
        result.append(f)
    return sorted(result, key=lambda f: f.start)


def analyze(text: str, *, quarantine_count: int) -> PiiResult:
    findings = _findings(text)
    categories = Counter(f.category for f in findings)
    reasons = sorted({f.reason for f in findings if f.action != ALLOW})
    if any(f.action == REJECT for f in findings):
        return PiiResult(REJECT, text, dict(sorted(categories.items())), tuple(reasons))
    to_redact = [f for f in findings if f.action == REDACT]
    if len(to_redact) >= quarantine_count:
        return PiiResult(QUARANTINE, text, dict(sorted(categories.items())),
                         tuple(reasons + ["personal_identifier_density"]))
    if not to_redact:
        return PiiResult(ALLOW, text, dict(sorted(categories.items())), tuple(sorted({f.reason for f in findings})))
    parts, cursor = [], 0
    for f in to_redact:
        parts.append(text[cursor:f.start])
        parts.append(f"[REDACTED_{f.category.upper()}]")
        cursor = f.end
    parts.append(text[cursor:])
    return PiiResult(REDACT, "".join(parts), dict(sorted(categories.items())), tuple(reasons), len(to_redact))
