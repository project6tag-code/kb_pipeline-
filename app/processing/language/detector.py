"""Gate 13: local English detection (LANGUAGE_DETECTOR_VERSION ``lang1``).

Layered, deterministic, dependency-free, fast on one PC:

1. **Script layer.** Letters are classified Latin / non-Latin. A text whose
   letters are mostly non-Latin is NON_ENGLISH without further work.
2. **Function-word identification.** Closed-class function words are the most
   frequent and the most language-specific tokens of running text. For 22
   Latin-script languages the detector counts tokens from each language's
   *exclusive* function-word list (words shared with English are removed from
   the other lists, and English uses only words no other list contains), so
   ``score[L] = hits[L] / tokens``. English confidence is
   ``score[en] / (score[en] + best other score)``, damped by the share of
   letters with diacritics (rare in English).
3. **Block layer** (before the document verdict). Paragraph blocks with enough
   tokens are classified on their own. The document is MIXED when non-English
   blocks hold at least ``mixed_fraction`` of the text and it is either English
   overall or its English blocks also hold at least ``mixed_fraction``.

Words that are function words in English *and* in another language (``i``,
``a``, ``in``, ...) are neutral: they count for no language.

Decisions: ENGLISH (confidence >= threshold, not mixed), NON_ENGLISH (another
language or script dominates), MIXED, UNRESOLVED (too short, too few function
words, or a confidence below the threshold). The Common Crawl index language
is recorded as a hint only; it never decides.

This is a function-word model, not a trained character-n-gram model: accuracy
on very short, list-like or tabular text is limited, which is why such text
is UNRESOLVED rather than guessed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

VERSION = "lang1"

_WORDS: dict[str, str] = {
    "en": "the of and to that is was for it with as his on be at by this had not are but from have they which "
          "you were her she there been one all would their we him has when who will more no if out so said what "
          "up its about into than them can only other new some could these two may then do first any my now such "
          "like our over me even most made after also did many before must through back years where much your way "
          "well down should because each just those how too little good very make world still own see men work "
          "long get here between both life being under never day same another know while last might us great old "
          "year off come since against go came right used take three however although therefore thus whether "
          "within without during upon among whose these those which would could should shall does doing done "
          "i a an in",
    "fr": "le la les des du une est et en que qui dans pour pas sur au aux avec ce cette il elle ils elles sont "
          "par plus ne se son sa ses ou mais nous vous leur été être fait comme tout bien aussi très même sans "
          "peut dont où ont était sont entre après avant sous chez leurs notre votre cela ces lui y je",
    "de": "der die und in den von zu das mit sich des auf für ist im dem nicht ein eine als auch es an werden "
          "aus er hat dass sie nach wird bei einer um am sind noch wie einem über einen so zum war haben nur oder "
          "aber vor zur bis mehr durch kann können wurde sein wenn ich wir ihr",
    "es": "el la de que y en los del se las por un para con no una su al lo como más pero sus le ya o este sí "
          "porque esta entre cuando muy sin sobre también me hasta hay donde quien desde todo nos durante todos "
          "uno les ni contra otros ese eso ante ellos e esto mí antes algunos qué unos yo otro otras otra él",
    "pt": "de a o que e do da em um para é com não uma os no se na por mais as dos como mas foi ao ele das tem "
          "à seu sua ou ser quando muito há nos já está eu também só pelo pela até isso ela entre era depois sem "
          "mesmo aos ter seus quem nas me esse eles estão você tinha foram essa num nem suas meu às minha",
    "it": "di e il la che è per un in del non una con sono le si della da gli al dei alla più anche ma lo come "
          "nel su ha delle questo ci era ne io nella se tra dal essere questa sua suo loro quando molto hanno "
          "sulla mi gli cui fa degli nelle già dopo essere tutto ancora così",
    "nl": "de het een en van ik te dat die in is niet op aan met als voor zijn er maar om hem dan zou of wat mijn "
          "men dit zo door over ze zich bij ook tot je mij uit der daar haar naar heb hoe heeft hebben deze u want "
          "nog zal me zij nu ge geen omdat iets worden toch al waren veel meer doen toen moet ben zonder kan hun "
          "dus alles onder ja eens hier wie werd altijd doch wordt wezen kunnen ons zelf tegen na reeds wil kon "
          "niets uw iemand geweest andere",
    "sv": "och det att i en jag hon som han på den med var sig för så till är men ett om hade de av icke mig du "
          "henne då sin nu har inte hans honom skulle hennes där min man ej vid kunde något från ut när efter upp "
          "vi dem vara vad över än dig kan sina här ha mot alla under någon eller allt mycket sedan ju denna själv "
          "detta åt utan varit hur ingen mitt ni bli blev oss din dessa några deras blir mina samma vilken er",
    "da": "og i jeg det at en den til er som på de med han af for ikke der var mig sig men et har om vi min havde "
          "ham hun nu over da fra du ud sin dem os op man hans hvor eller hvad skal selv her alle vil blev kunne "
          "ind når være dog noget ville jo deres efter ned skulle denne end dette mit også under have dig anden "
          "hende mine alt meget sit sine vor mod disse hvis din nogle hos blive mange ad bliver hendes været thi",
    "no": "og i jeg det at en et den til er som på de med han av ikke ikkje der så var meg seg men ett har om vi "
          "min mitt ha hadde hun nå over da ved fra du ut sin dem oss opp man kan hans hvor eller hva skal selv sjøl "
          "her alle vil bli ble blei kunne inn når være kom noen noe ville dere som deres kun ja etter ned skulle "
          "denne for deg si sine sitt mot å meget hvorfor dette disse uten hvordan ingen din ditt blir samme",
    "fi": "olla olen olet on olemme olette ovat ole oli olisi olisit olisin olisimme olisitte olisivat olit olin "
          "olimme olitte olivat ollut olleet en et ei emme ette eivät minä sinä hän me te he tämä tuo se nämä "
          "nuo ne kuka mikä joka jotka että ja jos koska kuin mutta niin sekä sillä tai vaan vai vaikka kanssa "
          "mukaan noin poikki yli kun niin nyt itse myös kuten",
    "pl": "i w z na się nie do to że jest o jak po co tak za od ale jego przez tylko już jej by był dla tym lub "
          "ich być go czy są może ten też jednak jeszcze bardzo gdy który która które będzie ma mnie mi były była "
          "było jako oraz pod przy tego tej te tu więc ze",
    "cs": "a se na v je že s to z do o i k by jsem jak ale tak jsou jako pro ve by bylo byl byla být jeho jen "
          "který která které když již jej od po při tak také tento tato toto ten tím mezi nebo pod podle proto "
          "protože před však už ze aby bude jejich",
    "tr": "ve bir bu da de için ile olarak çok daha gibi en o ne var mı ama ya kadar sonra her ben sen biz siz "
          "onlar şey değil olan oldu ise diye göre nasıl neden şu veya hem hiç bütün bazı ancak çünkü",
    "id": "yang dan di itu dengan untuk tidak ini dari dalam akan pada juga saya ke karena tersebut bisa ada "
          "mereka lebih kami kita oleh sudah atau seperti hanya telah dapat harus bahwa masih namun jika sangat "
          "banyak adalah sebagai orang semua saat",
    "ro": "și în de la a pe cu că nu o un ce se din mai care este sunt pentru sau dar lui fost fi ei ea ele eu "
          "noi voi am are au acest această acum când foarte după prin fără spre între despre ca",
    "hu": "a az és hogy nem is egy meg van de csak már el ez azt ki mint volt még ha vagy sem mert minden kell "
          "lesz után most ami aki pedig amely szerint nagyon között valamint amikor mind vele neki ezt",
    "vi": "của và các có là được trong cho những với không một này người đã để khi từ theo cũng đến như về "
          "nhiều tại sẽ ra thì nhưng vào nó đó rằng hay làm",
    "tl": "ang ng sa na mga at ay si ni kay para hindi ito siya ako ka nila niya mo namin natin sila ko kung "
          "din rin lang pa po ba nang dahil kaya pero",
    "ca": "el la els les de del i en que un una per amb no es són com més però va aquest aquesta hi ho seu seva "
          "també molt quan fins sobre entre sense dels",
    "sw": "na ya wa kwa ni za la katika kuwa hii kama pia au lakini sana hiyo huo yake wake hao wao sisi mimi "
          "wewe yeye bado baada kabla kila",
    "af": "die en van het is in wat nie om te dat vir met op sy hy ek ons was jy hulle maar ook sal kan word "
          "deur aan na tot hierdie daardie wanneer baie",
    "et": "ja on ei et see ka kui aga oli nii mis ta ma me te nad oma veel siis kõik mitte või juba seda selle "
          "ning sest pole olen",
    "lt": "ir yra į kad su iš ar bet kaip tai jis ji mes jūs jie o per apie buvo nuo taip tik dar kai jau tas "
          "savo labai kuris kuri",
}
_EN = frozenset(_WORDS["en"].split())
ENGLISH_FUNCTION_WORDS = _EN
_OTHER: dict[str, frozenset[str]] = {lang: frozenset(words.split()) - _EN for lang, words in _WORDS.items()
                                     if lang != "en"}
_EN_EXCLUSIVE = _EN - frozenset().union(*_OTHER.values())
_TOKEN = re.compile(r"[^\W\d_]+(?:['’][^\W\d_]+)?")
_FENCED = re.compile(r"^```.*?^```", re.S | re.M)


class Decision:
    ENGLISH = "ENGLISH"
    NON_ENGLISH = "NON_ENGLISH"
    MIXED = "MIXED"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True)
class LanguageResult:
    decision: str
    confidence: float
    language: str | None          # 'en' only for ENGLISH; best other language code otherwise (informational)
    tokens: int
    reasons: tuple[str, ...]
    scores: dict[str, float] = field(default_factory=dict)


def _is_latin(ch: str) -> bool:
    code = ord(ch)
    return code < 0x250 or 0x1E00 <= code <= 0x1EFF or 0x2C60 <= code <= 0x2C7F or 0xA720 <= code <= 0xA7FF


def _score(tokens: list[str]) -> tuple[float, str | None, float]:
    """(English score, best other language, its score)."""
    total = len(tokens)
    if total == 0:
        return 0.0, None, 0.0
    en = sum(1 for t in tokens if t in _EN_EXCLUSIVE) / total
    best, best_score = None, 0.0
    for lang in sorted(_OTHER):
        words = _OTHER[lang]
        score = sum(1 for t in tokens if t in words) / total
        if score > best_score:
            best, best_score = lang, score
    return en, best, best_score


def _classify(text: str) -> tuple[str, float, str | None, int, dict[str, float], list[str]]:
    letters = [ch for ch in text if ch.isalpha()]
    reasons: list[str] = []
    if not letters:
        return Decision.UNRESOLVED, 0.0, None, 0, {}, ["no_letters"]
    latin = sum(1 for ch in letters if _is_latin(ch)) / len(letters)
    tokens = [t.lower() for t in _TOKEN.findall(text)]
    if latin < 0.5:
        return Decision.NON_ENGLISH, 0.0, None, len(tokens), {"latin": round(latin, 3)}, ["non_latin_script"]
    en, other, other_score = _score(tokens)
    diacritics = sum(1 for ch in letters if ord(ch) > 0x7F) / len(letters)
    confidence = en / (en + other_score) if en + other_score > 0 else 0.0
    if diacritics > 0.03:
        confidence *= max(0.0, 1.0 - (diacritics - 0.03) * 5)
        reasons.append("diacritics")
    if latin < 0.9:
        confidence *= latin
        reasons.append("mixed_script")
    scores = {"en": round(en, 4), "latin": round(latin, 3)}
    if other:
        scores[other] = round(other_score, 4)
    decision = Decision.ENGLISH if en > other_score else Decision.NON_ENGLISH
    if en < 0.08 and other_score < 0.08:
        decision = Decision.UNRESOLVED
        reasons.append("low_function_words")
    return decision, confidence, other, len(tokens), scores, reasons


def detect(text: str, *, min_tokens: int, min_confidence: float, mixed_fraction: float,
           hint: str | None = None) -> LanguageResult:
    """Decide ENGLISH / NON_ENGLISH / MIXED / UNRESOLVED for ``text`` (fenced code is ignored)."""
    prose = _FENCED.sub(" ", text)
    decision, confidence, other, n_tokens, scores, reasons = _classify(prose)
    if hint:
        scores["cc_hint"] = 0.0 if "eng" not in hint.split(",") else 1.0
        if "eng" not in hint.split(","):
            reasons.append("cc_hint_not_eng")
    if n_tokens < min_tokens:
        return LanguageResult(Decision.UNRESOLVED, round(confidence, 4), None, n_tokens,
                              tuple(reasons + ["too_short"]), scores)
    # Block layer, before the document verdict: substantial foreign blocks in an English document, or
    # substantial English blocks in a foreign one, make the document MIXED.
    english = foreign = total = 0
    for block in prose.split("\n\n"):
        weight = len(block)
        total += weight
        if len(_TOKEN.findall(block)) < 15:
            continue
        block_decision = _classify(block)[0]
        if block_decision == Decision.ENGLISH:
            english += weight
        elif block_decision == Decision.NON_ENGLISH:
            foreign += weight
    if total and foreign / total >= mixed_fraction and (decision == Decision.ENGLISH
                                                         or english / total >= mixed_fraction):
        return LanguageResult(Decision.MIXED, round(confidence, 4), None, n_tokens,
                              tuple(reasons + [f"foreign_blocks={foreign / total:.2f}",
                                               f"english_blocks={english / total:.2f}"]), scores)
    if decision != Decision.ENGLISH:
        return LanguageResult(decision, round(confidence, 4), other if decision == Decision.NON_ENGLISH else None,
                              n_tokens, tuple(reasons), scores)
    if confidence < min_confidence:
        return LanguageResult(Decision.UNRESOLVED, round(confidence, 4), None, n_tokens,
                              tuple(reasons + ["low_confidence"]), scores)
    return LanguageResult(Decision.ENGLISH, round(min(confidence, 1.0), 4), "en", n_tokens, tuple(reasons), scores)

