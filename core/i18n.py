from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from string import Formatter
from typing import Final, TypeAlias, TypedDict

DEFAULT_UI_LANGUAGE: Final[str] = "ko"
SUPPORTED_UI_LANGUAGES: Final[tuple[str, ...]] = ("ko", "en")
I18N_DIR_NAME: Final[str] = "assets/i18n"

TranslationCatalog: TypeAlias = dict[str, str]


class TranslationBundle(TypedDict):
    project_root: Path
    language_code: str
    fallback_language_code: str
    catalog_dir: Path
    catalog_path: Path
    fallback_catalog_path: Path
    catalog: TranslationCatalog
    fallback_catalog: TranslationCatalog


class TranslationError(Exception):
    pass


class TranslationCatalogNotFoundError(FileNotFoundError, TranslationError):
    def __init__(self, path: Path) -> None:
        super().__init__(f"Translation catalog not found: {path}")
        self.path = path


class TranslationCatalogFormatError(ValueError, TranslationError):
    def __init__(self, path: Path, reason: str) -> None:
        super().__init__(f"Invalid translation catalog at {path}: {reason}")
        self.path = path
        self.reason = reason


class MissingTranslationKeyError(KeyError, TranslationError):
    def __init__(
        self,
        key: str,
        language_code: str,
        fallback_language_code: str,
        catalog_path: Path,
        fallback_catalog_path: Path,
    ) -> None:
        message = (
            "Missing translation key "
            f"key={key!r} language={language_code!r} fallback_language={fallback_language_code!r} "
            f"catalog_path={catalog_path!r} fallback_catalog_path={fallback_catalog_path!r}"
        )
        super().__init__(message)
        self.key = key
        self.language_code = language_code
        self.fallback_language_code = fallback_language_code
        self.catalog_path = catalog_path
        self.fallback_catalog_path = fallback_catalog_path


class TranslationFormatError(ValueError, TranslationError):
    def __init__(
        self,
        key: str,
        language_code: str,
        template: str,
        missing_fields: tuple[str, ...],
    ) -> None:
        fields_text = ", ".join(missing_fields)
        message = (
            "Translation format error "
            f"key={key!r} language={language_code!r} missing_fields=[{fields_text}] "
            f"template={template!r}"
        )
        super().__init__(message)
        self.key = key
        self.language_code = language_code
        self.template = template
        self.missing_fields = missing_fields


__all__ = (
    "DEFAULT_UI_LANGUAGE",
    "SUPPORTED_UI_LANGUAGES",
    "TranslationBundle",
    "TranslationCatalog",
    "TranslationCatalogFormatError",
    "TranslationCatalogNotFoundError",
    "TranslationError",
    "TranslationFormatError",
    "MissingTranslationKeyError",
    "I18N_DIR_NAME",
    "load_catalog",
    "load_translation_bundle",
    "normalize_language_code",
    "translate",
    "translate_kwargs",
)


def normalize_language_code(raw_value: str) -> str:
    normalized = raw_value.strip().lower().replace("_", "-")
    if not normalized:
        raise ValueError("language code must not be empty")
    return normalized


def _catalog_dir(project_root: Path) -> Path:
    return project_root / I18N_DIR_NAME


def _catalog_path(project_root: Path, language_code: str) -> Path:
    return _catalog_dir(project_root) / f"{normalize_language_code(language_code)}.json"


def _load_catalog_from_path(path: Path) -> TranslationCatalog:
    if not path.is_file():
        raise TranslationCatalogNotFoundError(path)

    try:
        raw_text = path.read_text(encoding="utf-8")
    except UnicodeError as exc:
        raise TranslationCatalogFormatError(path, "file is not valid UTF-8") from exc

    if raw_text.startswith("\ufeff"):
        raise TranslationCatalogFormatError(path, "UTF-8 BOM is not allowed")

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise TranslationCatalogFormatError(path, f"invalid JSON: {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise TranslationCatalogFormatError(path, "catalog root must be a JSON object")

    catalog: TranslationCatalog = {}
    invalid_entries: list[str] = []
    for raw_key, raw_value in payload.items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            invalid_entries.append(f"invalid key {raw_key!r}")
            continue
        if not isinstance(raw_value, str):
            invalid_entries.append(f"invalid value for {raw_key!r}")
            continue
        catalog[raw_key] = raw_value

    if invalid_entries:
        reason = "; ".join(invalid_entries)
        raise TranslationCatalogFormatError(path, reason)

    return catalog


def load_catalog(project_root: Path, language_code: str) -> TranslationCatalog:
    resolved_project_root = project_root.resolve()
    catalog_path = _catalog_path(resolved_project_root, language_code)
    return _load_catalog_from_path(catalog_path)


def load_translation_bundle(
    project_root: Path,
    language_code: str,
) -> TranslationBundle:
    resolved_project_root = project_root.resolve()
    normalized_language_code = normalize_language_code(language_code)
    normalized_fallback_language_code = DEFAULT_UI_LANGUAGE
    active_catalog_path = _catalog_path(resolved_project_root, normalized_language_code)
    fallback_catalog_path = _catalog_path(
        resolved_project_root,
        normalized_fallback_language_code,
    )

    catalog = _load_catalog_from_path(active_catalog_path)
    fallback_catalog = _load_catalog_from_path(fallback_catalog_path)

    return TranslationBundle(
        project_root=resolved_project_root,
        language_code=normalized_language_code,
        fallback_language_code=normalized_fallback_language_code,
        catalog_dir=_catalog_dir(resolved_project_root),
        catalog_path=active_catalog_path,
        fallback_catalog_path=fallback_catalog_path,
        catalog=catalog,
        fallback_catalog=fallback_catalog,
    )


def _lookup_template(
    bundle: TranslationBundle,
    key: str,
) -> tuple[str, str, Path]:
    normalized_key = key.strip()
    if not normalized_key:
        raise MissingTranslationKeyError(
            key=key,
            language_code=bundle["language_code"],
            fallback_language_code=bundle["fallback_language_code"],
            catalog_path=bundle["catalog_path"],
            fallback_catalog_path=bundle["fallback_catalog_path"],
        )

    active_catalog = bundle["catalog"]
    if normalized_key in active_catalog:
        return (
            active_catalog[normalized_key],
            bundle["language_code"],
            bundle["catalog_path"],
        )

    fallback_catalog = bundle["fallback_catalog"]
    if normalized_key in fallback_catalog:
        return (
            fallback_catalog[normalized_key],
            bundle["fallback_language_code"],
            bundle["fallback_catalog_path"],
        )

    raise MissingTranslationKeyError(
        key=normalized_key,
        language_code=bundle["language_code"],
        fallback_language_code=bundle["fallback_language_code"],
        catalog_path=bundle["catalog_path"],
        fallback_catalog_path=bundle["fallback_catalog_path"],
    )


def _format_translation(
    key: str,
    language_code: str,
    template: str,
    params: Mapping[str, object],
) -> str:
    formatter = Formatter()
    required_fields: set[str] = set()
    for _, field_name, _, _ in formatter.parse(template):
        if field_name is None or field_name == "":
            continue
        if "." in field_name or "[" in field_name or "]" in field_name:
            raise TranslationFormatError(
                key=key,
                language_code=language_code,
                template=template,
                missing_fields=(field_name,),
            )
        required_fields.add(field_name)

    missing_fields = tuple(sorted(name for name in required_fields if name not in params))
    if missing_fields:
        raise TranslationFormatError(
            key=key,
            language_code=language_code,
            template=template,
            missing_fields=missing_fields,
        )

    try:
        return template.format(**dict(params))
    except Exception as exc:
        raise TranslationFormatError(
            key=key,
            language_code=language_code,
            template=template,
            missing_fields=missing_fields,
        ) from exc


def translate(
    bundle: TranslationBundle,
    key: str,
    params: Mapping[str, object],
) -> str:
    template, language_code, _ = _lookup_template(bundle, key)
    return _format_translation(
        key=key,
        language_code=language_code,
        template=template,
        params=params,
    )


def translate_kwargs(bundle: TranslationBundle, key: str, **params: object) -> str:
    return translate(bundle, key, params)
