from __future__ import annotations

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
ZH_CN = DOCS / "zh-CN"
MARKDOWN_LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def documentation_pairs() -> list[tuple[Path, Path]]:
    pairs = [(ROOT / "README.md", ROOT / "README.zh-CN.md")]
    pairs.extend(
        (english, ZH_CN / english.relative_to(DOCS))
        for english in sorted(DOCS.rglob("*.md"))
        if ZH_CN not in english.parents
    )
    pairs.append(
        (ROOT / "e2e/kuberay/README.md", ROOT / "e2e/kuberay/README.zh-CN.md")
    )
    return pairs


def relative_link(source: Path, target: Path) -> str:
    return os.path.relpath(target, source.parent)


def test_every_document_has_english_and_simplified_chinese_versions() -> None:
    pairs = documentation_pairs()
    expected_chinese = {
        chinese
        for _, chinese in pairs
        if ZH_CN in chinese.parents or chinese == ZH_CN / "README.md"
    }
    actual_chinese = set(ZH_CN.rglob("*.md"))

    assert actual_chinese == expected_chinese
    for english, chinese in pairs:
        assert english.is_file(), f"missing English document: {english.relative_to(ROOT)}"
        assert chinese.is_file(), f"missing Chinese document: {chinese.relative_to(ROOT)}"


def test_every_document_has_a_language_switch_link() -> None:
    for english, chinese in documentation_pairs():
        english_text = english.read_text(encoding="utf-8")
        chinese_text = chinese.read_text(encoding="utf-8")
        assert f"({relative_link(english, chinese)})" in english_text
        assert f"({relative_link(chinese, english)})" in chinese_text


def test_local_markdown_links_resolve() -> None:
    documents = {path for pair in documentation_pairs() for path in pair}
    for document in documents:
        for raw_target in MARKDOWN_LINK.findall(document.read_text(encoding="utf-8")):
            target = raw_target.strip().split("#", 1)[0]
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            resolved = (document.parent / target).resolve()
            assert resolved.exists(), (
                f"broken link in {document.relative_to(ROOT)}: {raw_target}"
            )
