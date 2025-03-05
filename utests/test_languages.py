import pytest
import re
import string
from pathlib import Path

from extractor_service.common.struct.language import Language, LanguageEnum
from extractor_service.common.struct.word import WordList, Word
from extractor_service.extractor.languages.english_language import English
from extractor_service.extractor.languages.russian_language import Russian
from extractor_service.extractor.languages import kmp_search
from extractor_service.extractor.languages.language_facture import (
    EnglishFactory,
    RussianFactory,
    LanguageFactoryContainer,
    get_language_instance
)

NONEXISTENT = "NONEXISTENTWORD123"


@pytest.fixture
def temp_text_file(tmp_path) -> Path:
    """
    Фикстура для создания временного текстового файла.
    Возвращает путь к файлу.
    """
    file_path = tmp_path / "test_file.txt"
    return file_path


class TestLanguageStaticMethods:
    """
    Тестируем статические методы абстрактного класса Language.
    Т.к. Language – абстрактный класс, проверяем только
    доступные (статические) методы.
    """

    @pytest.mark.parametrize(
        "word_groups, expected",
        [

            ([["NASA"], ["i"], ["Hello", "world"], ["CPU"], ["Cat"]],
             [["NASA"], ["Hello", "world"], ["CPU"]]),

            ([["Hello", "World"], ["A"], ["B", "C", "D"], ["Ok"]],
             [["Hello", "World"], ["B", "C", "D"]]),

            ([["I"], ["We", "Are"], ["CPP"]],
             [["We", "Are"], ["CPP"]]),

            ([], []),
        ]
    )
    def test_remove_single_length_groups(self, word_groups, expected):
        result = Language.remove_single_length_groups(word_groups)
        assert result == expected

    @pytest.mark.parametrize(
        "abbreviation, index, group, expected_index",
        [

            ("NASA", 0, ["N", "A", "S", "A"], 0),

            ("ABC", 0, ["AbC", "def"], 0),

            ("NASA", 0, ["Some", "random", "N", "A", "S", "A", "text"], 2),

            ("CPU", 0, ["random", "words", "here"], 0),

            ("CAT", 2, ["Hello", "my", "C", "A", "T", "friend"], 2),
        ]
    )
    def test_normalize_match(self, abbreviation, index, group, expected_index):
        result = Language._normalize_match(abbreviation, index, group)
        assert result == expected_index

    @pytest.mark.parametrize(
        "words, expected",
        [
            (["Hello", "World"], ["Hello", "World"]),
            ([], []),
            (["One", "Two", "Three"], ["One", "Two", "Three"]),
        ]
    )
    def test_normalize_words_form(self, words, expected):
        result = Language.normalize_words_form(words)
        assert result == expected


class TestEnglish:
    """
    Тесты для класса English
    """

    def test_abbreviation_pattern(self):
        pattern = English().abbreviation_pattern

        assert isinstance(pattern, str)

        match = re.findall(pattern, "NASA CPU HELLO AbcD ABC123")
        assert match == ["NASA", "CPU", "HELLO"]

    def test_word_pattern(self):
        pattern = English().word_pattern
        text = "Hello world! 123 test"
        words = re.findall(pattern, text)
        assert words == ["Hello", "world", "test"]

    @pytest.mark.parametrize(
        "text, expected_abbr",
        [
            ("NASA is great. CPU is central processing unit. HELLO", ["NASA", "CPU", "HELLO"]),
            ("No abbreviations here", []),
            ("Some MixedCase abbreviations: ABC, AbC, CPU", ["ABC", "CPU"]),
        ]
    )
    def test_find_abbreviations(self, text, expected_abbr):
        eng = English()
        result = eng.find_abbreviations(text)
        assert result == expected_abbr

    def test_get_words_from_file(self, temp_text_file):
        text = """Hello, world!
                  This is a test-file for NASA, and CPU."""
        temp_text_file.write_text(text, encoding="utf-8")

        eng = English()
        words = eng.get_words_from_file(temp_text_file)

        assert "Hello" in words
        assert "world" in words
        assert "This" in words
        assert "is" in words
        assert "a" in words

    @pytest.mark.parametrize(
        "text, expected_words",
        [
            ("Hello, world!", ["Hello", "world"]),
            ("Some   text with---punctuation", ["Some", "text", "with", "punctuation"]),
            ("", []),
        ]
    )
    def test_get_words_from_string(self, text, expected_words):
        eng = English()
        result = eng.get_words_from_string(text)

        filtered = [w for w in result if w != NONEXISTENT]
        assert filtered == expected_words

    @pytest.mark.parametrize(
        "words, expected_groups",
        [
            (

                    ["Hello", "WORLD", "CPU", "Test", "Ok", "Some", "MixedCase", "AbC"],
                    [['MixedCase'], ['AbC'], ['Test', 'Ok', 'Some', 'MixedCase', 'AbC']]
            ),
            (
                    [],
                    []
            ),
            (

                    ["NASA"],
                    []
            ),
            (
                    ["hello", "there", "MY", "Friend", "123", "CPU", "X"],
                    []
            ),
        ]
    )
    def test_get_word_groups_from_wordlist(self, words, expected_groups):
        eng = English()
        result = eng.get_word_groups_from_wordlist(words)
        assert result == expected_groups

    def test_find_expansion(self):
        eng = English()
        abbreviations = ["NASA", "CPU"]
        word_groups = [
            ["National", "Aeronautics", "Space", "Administration"],
            ["Central", "Processing", "Unit"],
            ["Some", "Other", "Words"]
        ]
        word_list = ["Some", "Other", "Words", "National", "Aeronautics",
                     "Space", "Administration", "Central", "Processing", "Unit"]

        expansions = list(eng.find_expansion(abbreviations, word_groups, word_list))

        assert ("NASA", ["National", "Aeronautics", "Space", "Administration"]) in expansions
        assert ("CPU", ["Central", "Processing", "Unit"]) in expansions

        assert len(expansions) == 2


class TestRussian:
    """
    Тесты для класса Russian
    """

    def test_abbreviation_pattern(self):
        pattern = Russian().abbreviation_pattern
        text = "РАН ООН США ННН Abc"

        matches = re.findall(pattern, text)
        assert matches == ["РАН", "ООН", "США", "ННН"]

    def test_word_pattern(self):
        pattern = Russian().word_pattern
        text = "Привет, мир! 123"
        words = re.findall(pattern, text)
        assert words == ["Привет", "мир"]

    @pytest.mark.parametrize(
        "text, expected",
        [
            ("Привет, это РАН и ООН!", ["РАН", "ООН"]),
            ("Слов нет", []),
            ("1234", []),
        ]
    )
    def test_find_abbreviations(self, text, expected):
        rus = Russian()
        result = rus.find_abbreviations(text)
        assert result == expected

    def test_get_words_from_file(self, temp_text_file):
        text = """Привет, это тестовый файл.
                  Здесь ООН, а также РАН. И цифры 1234"""
        temp_text_file.write_text(text, encoding="utf-8")

        rus = Russian()
        words = rus.get_words_from_file(temp_text_file)

        assert "ООН" not in words
        assert "РАН" not in words

        assert "Привет" in words
        assert "это" in words
        assert "тестовый" in words

    @pytest.mark.parametrize(
        "text, expected",
        [
            ("Привет, это РАН!", ["Привет", "это"]),
            ("---", []),
            ("Просто текст 1234 и ООН", ["Просто", "текст", "1234", "и"]),
        ]
    )
    def test_get_words_from_string(self, text, expected):
        rus = Russian()
        result = rus.get_words_from_string(text)
        filtered = [w for w in result if w != NONEXISTENT]
        assert filtered == expected

    @pytest.mark.parametrize(
        "words, expected_groups",
        [
            (

                    ["Привет", "мир", "ООН", "тест", "Тут", "ещё", "что-то"],
                    [['Привет', 'мир', 'ООН', 'тест', 'Тут', 'ещё', 'что-то']]
            ),
            (
                    ["одиночка", "два", "слова"],
                    [["одиночка", "два", "слова"]]
            ),
            (
                    [],
                    []
            ),
        ]
    )
    def test_get_word_groups_from_wordlist(self, words, expected_groups):
        rus = Russian()
        result = rus.get_word_groups_from_wordlist(words)
        set_result = set(tuple(g) for g in result)
        set_expected = set(tuple(g) for g in expected_groups)
        assert set_result == set_expected

    def test_find_expansion(self):
        rus = Russian()
        abbreviations = ["РАН"]
        word_groups = [
            ["Российская", "Академия", "Наук"],
            ["Просто", "слова"],
            ["Академия", "Наук", "Российская"]
        ]
        word_list = ["Российская", "Академия", "Наук", "Просто", "слова"]

        expansions = list(rus.find_expansion(abbreviations, word_groups, word_list))

        assert ("РАН", ["Российская", "Академия", "Наук"]) in expansions

    @pytest.mark.parametrize(
        "words, expected_normalized",
        [
            (["Привет", "Мира"], ["привет", "мир"]),
            (["Столы", "стул"], ["стол", "стул"]),
        ]
    )
    def test_normalize_words_form(self, words, expected_normalized):
        rus = Russian()
        result = rus.normalize_words_form(words)
        assert result == expected_normalized


class TestLanguageFactories:
    """
    Тестируем фабрики и функцию get_language_instance
    """

    def test_english_factory(self):
        lang = EnglishFactory.create_language()
        from extractor_service.extractor.languages.english_language import English
        assert isinstance(lang, English)

    def test_russian_factory(self):
        lang = RussianFactory.create_language()
        from extractor_service.extractor.languages.russian_language import Russian
        assert isinstance(lang, Russian)

    def test_language_factory_container(self):
        factory = LanguageFactoryContainer.get_factory(LanguageEnum.ENGLISH)
        assert factory == EnglishFactory

        factory = LanguageFactoryContainer.get_factory(LanguageEnum.RUSSIAN)
        assert factory == RussianFactory

        class FakeEnum:
            value = "fake"

        assert LanguageFactoryContainer.get_factory(FakeEnum) is None

    @pytest.mark.parametrize("lang_enum, expected_class", [
        (LanguageEnum.ENGLISH, English),
        (LanguageEnum.RUSSIAN, Russian),
    ])
    def test_get_language_instance(self, lang_enum, expected_class):
        instance = get_language_instance(lang_enum)
        assert isinstance(instance, expected_class)

    def test_get_language_instance_invalid(self):
        with pytest.raises(ValueError):
            get_language_instance(LanguageEnum("invalid"))


if __name__ == "__main__":
    pytest.main()
