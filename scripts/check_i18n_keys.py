from __future__ import annotations

import argparse
import ast
import json
import sys
from collections import defaultdict
from pathlib import Path
from string import Formatter
from typing import Any


REQUIRED_LANGUAGES = ("ko", "en")
IGNORED_DIR_NAMES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "venv",
}
TRANSLATION_FUNCTION_NAMES = {"_", "gettext", "t", "tr", "translate"}


def load_json_file(file_path: Path) -> Any:
    with file_path.open("r", encoding="utf-8-sig") as file_handle:
        return json.load(file_handle)


def flatten_catalog(node: Any, prefix: str, source_path: Path) -> dict[str, str]:
    if isinstance(node, dict):
        flattened: dict[str, str] = {}
        for raw_key in sorted(node):
            if not isinstance(raw_key, str):
                raise TypeError(f"키는 문자열이어야 합니다: {source_path} -> {raw_key!r}")
            next_prefix = raw_key if not prefix else f"{prefix}.{raw_key}"
            nested = flatten_catalog(node[raw_key], next_prefix, source_path)
            flattened.update(nested)
        return flattened

    if isinstance(node, str):
        if not prefix:
            raise ValueError(f"최상위 번역 값은 문자열이 아니라 객체여야 합니다: {source_path}")
        return {prefix: node}

    raise TypeError(f"번역 값은 문자열 또는 중첩 객체여야 합니다: {source_path} -> {type(node).__name__}")


def discover_locale_files(locale_dir: Path) -> list[Path]:
    if not locale_dir.exists():
        raise FileNotFoundError(f"locale 디렉터리를 찾을 수 없습니다: {locale_dir}")
    if not locale_dir.is_dir():
        raise NotADirectoryError(f"locale 경로가 디렉터리가 아닙니다: {locale_dir}")

    locale_files = sorted(path for path in locale_dir.glob("*.json") if path.is_file())
    if not locale_files:
        raise FileNotFoundError(f"locale JSON 파일이 없습니다: {locale_dir}")
    return locale_files


def load_locale_catalogs(locale_dir: Path) -> dict[str, dict[str, str]]:
    catalogs: dict[str, dict[str, str]] = {}
    for locale_file in discover_locale_files(locale_dir):
        language_code = locale_file.stem
        catalogs[language_code] = flatten_catalog(load_json_file(locale_file), "", locale_file)
    return catalogs


def extract_placeholders(template: str, source_path: Path, key: str) -> set[str]:
    formatter = Formatter()
    placeholders: set[str] = set()

    try:
        for _, field_name, _, _ in formatter.parse(template):
            if field_name is None:
                continue
            if field_name == "":
                raise ValueError
            placeholders.add(field_name)
    except ValueError as exc:
        raise ValueError(f"잘못된 format 문자열입니다: {source_path} -> {key}") from exc

    return placeholders


def validate_locale_catalogs(catalogs: dict[str, dict[str, str]], locale_dir: Path) -> list[str]:
    errors: list[str] = []

    for language_code in REQUIRED_LANGUAGES:
        if language_code not in catalogs:
            errors.append(f"필수 locale 파일이 없습니다: {locale_dir / f'{language_code}.json'}")

    if errors:
        return errors

    baseline_language = "ko"
    baseline_catalog = catalogs[baseline_language]
    baseline_keys = set(baseline_catalog)

    for language_code in sorted(catalogs):
        catalog = catalogs[language_code]
        catalog_keys = set(catalog)
        missing_keys = sorted(baseline_keys - catalog_keys)
        extra_keys = sorted(catalog_keys - baseline_keys)
        if missing_keys:
            errors.append(
                f"{language_code}.json 에 누락된 key가 있습니다: {', '.join(missing_keys)}"
            )
        if extra_keys:
            errors.append(
                f"{language_code}.json 에 기준에 없는 key가 있습니다: {', '.join(extra_keys)}"
            )

    placeholder_baseline: dict[str, set[str]] = {}
    for key in sorted(baseline_keys):
        placeholder_baseline[key] = extract_placeholders(baseline_catalog[key], locale_dir / "ko.json", key)

    for language_code in sorted(catalogs):
        if language_code == baseline_language:
            continue
        catalog = catalogs[language_code]
        for key in sorted(baseline_keys):
            current_placeholders = extract_placeholders(catalog[key], locale_dir / f"{language_code}.json", key)
            if current_placeholders != placeholder_baseline[key]:
                errors.append(
                    f"{language_code}.json 의 placeholder가 일치하지 않습니다: {key} "
                    f"(ko={sorted(placeholder_baseline[key])}, {language_code}={sorted(current_placeholders)})"
                )

    return errors


def is_ignored_python_path(file_path: Path, root_path: Path) -> bool:
    relative_parts = file_path.relative_to(root_path).parts
    for part in relative_parts[:-1]:
        if part in IGNORED_DIR_NAMES or part.startswith("_MEI"):
            return True
    return False


def discover_python_sources(root_path: Path) -> list[Path]:
    sources: list[Path] = []
    for file_path in sorted(root_path.rglob("*.py")):
        if file_path.is_file() and not is_ignored_python_path(file_path, root_path):
            sources.append(file_path)
    return sources


def read_source_text(source_path: Path) -> str:
    try:
        return source_path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        return source_path.read_text(encoding="cp949", errors="ignore")


def collect_referenced_keys(root_path: Path) -> dict[str, list[Path]]:
    references: dict[str, list[Path]] = defaultdict(list)
    for source_path in discover_python_sources(root_path):
        source_text = read_source_text(source_path)
        try:
            tree = ast.parse(source_text, filename=str(source_path))
        except SyntaxError as exc:
            raise SyntaxError(f"Python 구문 오류로 i18n 검사를 진행할 수 없습니다: {source_path}") from exc

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            function_name = get_translation_function_name(node.func)
            if function_name not in TRANSLATION_FUNCTION_NAMES:
                continue
            if not node.args:
                continue

            first_argument = node.args[0]
            if not isinstance(first_argument, ast.Constant) or not isinstance(first_argument.value, str):
                continue

            references[first_argument.value].append(source_path)

    return references


def get_translation_function_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def validate_source_references(
    references: dict[str, list[Path]],
    catalog_keys: set[str],
) -> list[str]:
    errors: list[str] = []
    for key in sorted(references):
        if key not in catalog_keys:
            reference_files = sorted({str(path) for path in references[key]})
            errors.append(
                f"소스에서 참조하지만 locale catalog에 없는 key가 있습니다: {key} "
                f"({', '.join(reference_files)})"
            )
    return errors


def parse_arguments() -> argparse.Namespace:
    argument_parser = argparse.ArgumentParser(description="locale key 일치 여부를 검사합니다.")
    argument_parser.add_argument(
        "--root",
        dest="root_path",
        required=False,
        help="검사할 repository root 경로",
    )
    argument_parser.add_argument(
        "--locale-dir",
        dest="locale_dir_path",
        required=False,
        help="locale JSON 디렉터리 경로",
    )
    return argument_parser.parse_args()


def resolve_default_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    arguments = parse_arguments()
    root_path = Path(arguments.root_path).resolve() if arguments.root_path else resolve_default_root()
    locale_dir = Path(arguments.locale_dir_path).resolve() if arguments.locale_dir_path else root_path / "assets" / "i18n"

    catalogs = load_locale_catalogs(locale_dir)
    validation_errors = validate_locale_catalogs(catalogs, locale_dir)

    baseline_catalog = catalogs["ko"]
    referenced_keys = collect_referenced_keys(root_path)
    validation_errors.extend(validate_source_references(referenced_keys, set(baseline_catalog)))

    if validation_errors:
        for validation_error in validation_errors:
            print(f"[ERROR] {validation_error}", file=sys.stderr)
        return 1

    print(
        "[INFO] i18n 검증 완료: "
        f"languages={', '.join(sorted(catalogs))}, "
        f"keys={len(baseline_catalog)}, "
        f"referenced_keys={len(referenced_keys)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
