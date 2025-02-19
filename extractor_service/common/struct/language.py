from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import List, Tuple

from extractor_service.common.struct.word import WordList, Word
from extractor_service.extractor.languages import kmp_search


class LanguageEnum(Enum):
    ENGLISH = "en"
    RUSSIAN = "ru"

    def __str__(self):
        return self.value


class Language(ABC):
    def __init__(self):
        self._abbreviation_pattern = None

    @property
    @abstractmethod
    def abbreviation_pattern(self) -> str:
        """ Паттерн для поиска аббревиатур в тексте. """
        pass

    @property
    @abstractmethod
    def word_pattern(self) -> str:
        """ Паттерн для поиска слов в тексте. """
        pass

    @abstractmethod
    def find_abbreviations(self, text: str) -> List[str]:
        """ Находит аббревиатуры в тексте.

        :param text: Текст, в котором нужно искать аббревиатуры.
        :return: Список найденных аббревиатур.
        """
        pass

    @abstractmethod
    def get_words_from_file(self, file_path: Path) -> List[str]:
        """ Извлекает слова из текстового файла.

        :param file_path: Путь к текстовому файлу.
        :return: Список всех слов, извлеченных из текстового файла.
        """
        pass

    @abstractmethod
    def get_words_from_string(self, text: str) -> List[str]:
        pass

    @abstractmethod
    def get_word_groups_from_wordlist(self, words: List[str]) -> List[List[str]]:
        """ Извлекает группы слов из текстового файла.

        :param words: Список всех слов.
        :return: Группы слов.
        """
        pass

    @staticmethod
    def remove_single_length_groups(word_groups: List[List[str]]) -> List[List[str]]:
        """ Удаляет группы слов длиной 1.

        :param word_groups: Список групп слов.
        :return: Список групп слов без групп длиной 1.
        """
        result = []
        for word_group in word_groups:
            if len(word_group) > 1 or (
                    len(word_group) == 1
                    and len(Word.get_uppercase_letters(word_group[0])) > 1
            ):
                result.append(word_group)
        return result

    @abstractmethod
    def find_expansion(
            self,
            abbreviations: List[str],
            word_groups: List[List[str]],
            word_list: List[str],
    ) -> Tuple[str, List[str]]:
        """ Находит расшифровку аббревиатуры.

        :param abbreviations: Список аббревиатур.
        :param word_groups: Список групп слов.
        :param word_list: Список всех слов.
        :return: Кортеж из аббревиатуры и ее расшифровки.
        """
        pass

    @staticmethod
    def _normalize_match(abbreviation: str, index: int, group: List[str]) -> int:
        """ Проверяет корректность найденного индекса (index) аббревиатуры (abbreviation) в списке слов (group).

        :param abbreviation: Аббревиатура, которую необходимо найти в группе слов.
        :param index: Индекс вхождения аббревиатуры в группе слов.
        :param group: Список слов, в котором производится поиск аббревиатуры.
        :returns: Скорректированный индекс вхождения аббревиатуры в группе слов, если вхождение
                  действительно найдено, в противном случае возвращается -1.
        """
        if (
                index + len(abbreviation) <= len(group)
                and WordList.get_first_and_capital_letters_from_wordlist(
            group[index: index + len(abbreviation)]
        ).lower()
                == abbreviation.lower()
        ):
            return index
        matches = kmp_search.kmp_search(
            WordList.get_first_letters_from_wordlist(group).lower(),
            abbreviation.lower(),
        )
        if matches:
            return matches[0]

        words = [Word.get_first_and_capital_letters(word) for word in group]
        for i, word in enumerate(words):
            if index >= len(word):
                index -= len(word)

            if index == 0:
                return i

            return -1

    @staticmethod
    def normalize_words_form(words: List[str]) -> List[str]:
        return words