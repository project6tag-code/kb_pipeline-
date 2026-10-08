"""Raw LaTeX and wiki-markup to readable text (MARKUP_NORMALIZER_VERSION ``mk1``).

Runs in Silver on prose / HTML / Markdown text (never code or config), before
the symbol cleanup and canonicalization. It rewrites only what has an exact
plain-text reading, and leaves anything else untouched:

* math spans ``$...$``, ``$$...$$``, ``\\(...\\)``, ``\\[...\\]`` that contain LaTeX
  (a backslash command, ``^``, ``_`` or braces) are rewritten as plain text:
  ``\\alpha`` -> ``α``, ``\\leq`` -> ``≤``, ``\\times`` -> ``×``, ``\\frac{a}{b}`` ->
  ``a/b``, ``\\sqrt{x}`` -> ``√(x)``, ``\\text{kg}`` -> ``kg``, ``x^{2}`` -> ``x^2``,
  ``H_{2}O`` -> ``H_2O``; spacing commands are dropped;
* a span is rewritten only when EVERY command in it is known: one unknown
  command (``\\mathcal``, ``\\begin{...}``) keeps the whole span as written, so no
  meaning is lost or invented. Currency such as ``$5 and $10`` has no LaTeX and is
  never a math span;
* wiki markup: ``[[target|label]]`` -> ``label``, ``[[target]]`` -> ``target``,
  ``'''bold'''`` and ``''italic''`` -> the text.
"""

from __future__ import annotations

import re

MARKUP_NORMALIZER_VERSION = "mk1"

_SYMBOLS = {
    "alpha": "α", "beta": "β", "gamma": "γ", "delta": "δ", "epsilon": "ε", "varepsilon": "ε", "zeta": "ζ",
    "eta": "η", "theta": "θ", "iota": "ι", "kappa": "κ", "lambda": "λ", "mu": "μ", "nu": "ν", "xi": "ξ",
    "pi": "π", "rho": "ρ", "sigma": "σ", "tau": "τ", "upsilon": "υ", "phi": "φ", "varphi": "φ", "chi": "χ",
    "psi": "ψ", "omega": "ω", "Gamma": "Γ", "Delta": "Δ", "Theta": "Θ", "Lambda": "Λ", "Xi": "Ξ", "Pi": "Π",
    "Sigma": "Σ", "Phi": "Φ", "Psi": "Ψ", "Omega": "Ω",
    "times": "×", "cdot": "·", "pm": "±", "mp": "∓", "div": "÷", "leq": "≤", "le": "≤", "geq": "≥", "ge": "≥",
    "neq": "≠", "ne": "≠", "approx": "≈", "equiv": "≡", "sim": "∼", "propto": "∝", "infty": "∞",
    "rightarrow": "→", "to": "→", "leftarrow": "←", "leftrightarrow": "↔", "Rightarrow": "⇒", "Leftrightarrow": "⇔",
    "sum": "∑", "prod": "∏", "int": "∫", "partial": "∂", "nabla": "∇", "in": "∈", "notin": "∉",
    "subset": "⊂", "subseteq": "⊆", "cup": "∪", "cap": "∩", "forall": "∀", "exists": "∃", "emptyset": "∅",
    "degree": "°", "circ": "∘", "ldots": "…", "cdots": "…", "dots": "…", "ll": "≪", "gg": "≫", "angle": "∠",
    "perp": "⊥", "parallel": "∥", "hbar": "ħ", "ell": "ℓ", "sin": "sin", "cos": "cos", "tan": "tan",
    "log": "log", "ln": "ln", "exp": "exp", "min": "min", "max": "max", "lim": "lim", "det": "det",
}
_DROP = frozenset({"left", "right", "displaystyle", "textstyle", "big", "Big", "bigg", "Bigg", ",", ";", ":", "!",
                   " ", "quad", "qquad", "limits", "nolimits"})
_WRAPPERS = ("text", "textbf", "textit", "mathrm", "mathbf", "mathit", "mathsf", "operatorname", "boldsymbol",
             "mbox")
_MATH = re.compile(r"\$\$(.+?)\$\$|(?<![\\$])\$(?!\s)([^$\n]{1,300}?)(?<!\s)\$(?!\d)|\\\((.+?)\\\)|\\\[(.+?)\\\]", re.S)
_COMMAND = re.compile(r"\\([A-Za-z]+|[,;:! ])")
_SCRIPT = re.compile(r"([\^_])\{([^{}]{1,40})\}")
_FRAC = re.compile(r"\\[dt]?frac\{([^{}]{1,60})\}\{([^{}]{1,60})\}")
_SQRT = re.compile(r"\\sqrt\{([^{}]{1,60})\}")
_WRAP = re.compile(r"\\(" + "|".join(_WRAPPERS) + r")\{([^{}]{1,80})\}")
_WIKI_LINK = re.compile(r"\[\[(?:[^\]|\[]{1,200}\|)?([^\]|\[]{1,200})\]\]")
_WIKI_BOLD = re.compile(r"'''([^'\n]{1,200})'''")
_WIKI_ITALIC = re.compile(r"(?<!')''([^'\n]{1,200})''(?!')")
_LATEX_HINT = re.compile(r"\\[A-Za-z]|[\^_]\{|\{|\\[,;:! ]|[A-Za-z0-9)][\^_][A-Za-z0-9({]")


def _atom(value: str) -> str:
    value = value.strip()
    return value if re.fullmatch(r"[\w.]+", value) else f"({value})"


def _math_to_text(body: str) -> str | None:
    """Plain text of one math span, or None when it holds a command with no exact plain reading."""
    work = body
    for _ in range(3):  # nested wrappers / fractions
        work = _FRAC.sub(lambda m: f"{_atom(m.group(1))}/{_atom(m.group(2))}", work)
        work = _SQRT.sub(lambda m: "√" + _atom(m.group(1)), work)
        work = _WRAP.sub(lambda m: m.group(2), work)
    work = _SCRIPT.sub(lambda m: f"{m.group(1)}{m.group(2)}" if re.fullmatch(r"[\w+-]{1,40}", m.group(2))
                       else f"{m.group(1)}({m.group(2)})", work)

    def command(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in _DROP:
            return ""
        if name in _SYMBOLS:
            return _SYMBOLS[name]
        raise KeyError(name)

    try:
        work = _COMMAND.sub(command, work)
    except KeyError:
        return None
    if "\\" in work or "{" in work or "}" in work:
        return None
    return re.sub(r"\s+", " ", work).strip()


def _replace_math(match: re.Match[str]) -> str:
    body = next(group for group in match.groups() if group is not None)
    if not _LATEX_HINT.search(body):
        return match.group(0)  # currency or plain dollars, not LaTeX
    text = _math_to_text(body)
    return match.group(0) if not text else text


def normalize(text: str) -> str:
    """Readable text for LaTeX math spans and wiki markup; everything else unchanged. Idempotent."""
    if "\\" in text or "$" in text:
        text = _MATH.sub(_replace_math, text)
    if "[[" in text or "''" in text:
        text = _WIKI_LINK.sub(r"\1", text)
        text = _WIKI_BOLD.sub(r"\1", text)
        text = _WIKI_ITALIC.sub(r"\1", text)
    return text
