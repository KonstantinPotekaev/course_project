import json
import pytest
from unittest.mock import AsyncMock

from extractor_service.technologies.abbreviation_extraction.utils.abbreviation_extraction import extract

from extractor_service.common.func.misc import S3ContentType
from extractor_service.common.struct.language import LanguageEnum
from extractor_service.common.struct.model.abbreviation_extractor import ExpansionToSave


@pytest.mark.asyncio
async def test_extract_function():
    abbreviation_detector_model = AsyncMock()
    expansion_detector_model = AsyncMock()

    dummy_abbreviations_obj = type("DummyAbbr", (), {"abbreviations": ["ABBR1", "ABBR2"]})()
    dummy_expansions_obj = type("DummyExp", (), {"expansions": {"ABBR1": {"expansion1": 1},
                                                                "ABBR2": {"expansion2": 2}}})()

    abbreviation_detector_model.detect_abbreviations.return_value = dummy_abbreviations_obj
    expansion_detector_model.detect_expansions.return_value = dummy_expansions_obj

    content_id = "test_content"
    text = "Sample text for testing."
    language = LanguageEnum.RUSSIAN

    result_generator = extract(content_id, text, abbreviation_detector_model, expansion_detector_model, language)
    results = [result async for result in result_generator]

    assert len(results) == 1
    result_obj = results[0]

    expected_json = json.dumps(dummy_expansions_obj.expansions, ensure_ascii=False, indent=2)
    expected_bytes = expected_json.encode("utf-8")
    expected_length = len(expected_bytes)

    assert result_obj.key_ == content_id

    assert result_obj.file_data.getvalue() == expected_bytes
    assert result_obj.data_length == expected_length
    assert result_obj.file_type == S3ContentType.JSON

    abbreviation_detector_model.detect_abbreviations.assert_awaited_once_with(
        content_id=content_id, text=text, language=language
    )
    expansion_detector_model.detect_expansions.assert_awaited_once_with(
        content_id=content_id, text=text, language=language, abbreviations=dummy_abbreviations_obj.abbreviations
    )


def test_abbreviation_detector(monkeypatch):
    """
    Тест для класса AbbreviationDetector.
    Подменяем функцию get_language_instance, чтобы вернуть фиктивный язык с нужным методом.
    """

    class DummyLanguage:
        def find_abbreviations(self, text: str) -> list:
            return ["TEST", "ABC"]

    def dummy_get_language_instance(language):
        return DummyLanguage()

    monkeypatch.setattr(
        "extractor_service.extractor.abbreviation_detection.get_language_instance",
        dummy_get_language_instance
    )

    from extractor_service.extractor.abbreviation_detection import AbbreviationDetector
    detector = AbbreviationDetector()
    result = detector.detect("Some text with TEST and ABC", LanguageEnum.RUSSIAN)
    assert result == ["TEST", "ABC"]


def test_expansion_detector(monkeypatch):
    """
    Тест для класса ExpansionDetector.
    Подменяем get_language_instance, чтобы вернуть фиктивный язык, реализующий необходимые методы.
    """

    class DummyLanguage:
        def get_words_from_string(self, text: str) -> list:
            return text.split()

        def get_word_groups_from_wordlist(self, words: list) -> list:
            return [words]

        def remove_single_length_groups(self, groups: list) -> list:
            return groups

        def find_expansion(self, abbreviations: list, word_groups: list, word_list: list):
            for abbr in abbreviations:
                yield abbr, word_groups[0]

        def normalize_words_form(self, words: list) -> list:
            return [w.lower() for w in words]

    def dummy_get_language_instance(language):
        return DummyLanguage()

    monkeypatch.setattr(
        "extractor_service.extractor.expansion_detection.get_language_instance",
        dummy_get_language_instance
    )
    from extractor_service.extractor.expansion_detection import ExpansionDetector
    detector = ExpansionDetector()
    text = "This is a test text"
    abbreviations = ["TEST"]
    result = detector.detect(text, abbreviations, LanguageEnum.RUSSIAN)

    expected_group = " ".join([w.lower() for w in text.split()])
    assert "TEST" in result

    assert result["TEST"].get(expected_group, 0) > 0


if __name__ == "__main__":
    pytest.main()
