import re
import string
from pathlib import Path
from typing import List, Tuple

from extractor_service.common.struct.language import Language
from extractor_service.common.struct.word import WordList, Word
from extractor_service.extractor.languages import kmp_search


class English(Language):
    def __init__(self):
        super().__init__()

    @property
    def abbreviation_pattern(self) -> str:
        return r"\b[A-Z]{2,5}\b"

    @property
    def word_pattern(self) -> str:
        """ Возвращает шаблон для поиска английских слов в тексте.

        :return: Шаблон для поиска английских слов.
        """
        return r"\b[A-Za-z]+\b"

    def find_abbreviations(self, text: str) -> List[str]:
        """ Находит английские аббревиатуры в тексте.

        :param text: текст, в котором нужно искать аббревиатуры
        :return: итерируемый объект, содержащий английские аббревиатуры, найденные в тексте
        """
        abbreviations = re.findall(self.abbreviation_pattern, text)

        return abbreviations

    def get_words_from_file(self, file_path: Path) -> List[str]:
        """ Извлекает английские слова из текстового файла.

        :param file_path: Путь к текстовому файлу.
        :return: Список всех слов, извлеченных из текстового файла.
        """

        with open(file_path, "r+", encoding="utf-8") as file:
            text = file.read()
            text = text.replace("\n", " ")
            text = text.replace("\t", " ")

            text = re.sub(r"\s+", " ", text).strip()

        words = text.split()
        cleaned_words = []

        for word in words:
            cleaned_word = word
            # Удаляем все специальные символы из слова и добавляем его в список cleaned_words.
            for punctuation in string.punctuation + "«»":
                cleaned_word = cleaned_word.replace(punctuation, Word.NONEXISTENT_WORD)

            cleaned_words += cleaned_word.split()

        cleaned_words = [word for word in cleaned_words if word]
        return cleaned_words

    def get_words_from_string(self, text: str) -> List[str]:
        text = text.replace("\n", " ").replace("\t", " ")
        text = re.sub(r"\s+", " ", text).strip()

        words = text.split()
        cleaned_words = []
        for word in words:
            cleaned_word = word
            # Удаляем все специальные символы из слова и добавляем его в список cleaned_words.
            for punctuation in string.punctuation + "«»":
                cleaned_word = cleaned_word.replace(punctuation, Word.NONEXISTENT_WORD)

            cleaned_words += cleaned_word.split()

        cleaned_words = [word for word in cleaned_words if word]
        return cleaned_words

    def get_word_groups_from_wordlist(self, words: List[str]) -> List[List[str]]:
        """ Находит подряд идущие английские слова и объединяет их в группу.

        :param words: Путь к текстовому файлу.
        :returns: Группы слов.
        """

        word_groups = []
        current_group = []

        for word in words:
            if Word.is_english_word(word) and Word.is_valid_word(word):
                if sum(1 for letter in word if letter.isupper()) > 1:
                    word_groups.append([word])
                current_group.append(word)
            elif current_group:
                if (len(current_group) == 1 and len(current_group[0]) == 1) or len(
                        current_group
                ) == 1:
                    current_group = []
                    continue
                word_groups.append(current_group)
                current_group = []

        if len(current_group) > 1:
            word_groups.append(current_group)

        return word_groups

    def find_expansion(
            self,
            abbreviations: List[str],
            word_groups: List[List[str]],
            word_list: List[str],
    ) -> Tuple[str, List[str]]:
        """ Находит расшифровки аббревиатур на основе списка аббревиатур, групп слов и списка всех слов.

        :param abbreviations: Список аббревиатур, для которых нужно найти расшифровки.
        :param word_groups: Список групп слов, из которых могут состоять расшифровки.
        :param word_list: Список всех слов, в котором производится поиск расшифровок.
        :return: Кортеж, содержащий аббревиатуру и список расшифровок.
        """

        for abbreviation in abbreviations:
            for index, group in enumerate(word_groups):

                uppercase_letters = WordList.get_uppercase_letters_from_wordlist(group)

                if uppercase_letters == abbreviation:
                    yield abbreviation, group
                    continue

                # Проверка совпадения аббревиатуры с первыми буквами
                first_letters = WordList.get_first_letters_from_wordlist(group)
                matches = kmp_search.kmp_search(
                    first_letters.lower(), abbreviation.lower()
                )

                for match in matches:
                    sub_group = group[match: match + len(abbreviation)]
                    yield abbreviation, sub_group

                # Если есть совпадения, пропустить
                if matches:
                    continue

                # При использовании этой переменной для поиска расшифровки стоит
                # отметить, что некоторые буквы
                # могут быть взяты из одного слова. Требуется проверка!
                first_and_capital_letters = (
                    WordList.get_first_and_capital_letters_from_wordlist(group)
                )

                # Пропустить, если длина меньше длины аббревиатуры
                if len(first_and_capital_letters) < len(abbreviation):
                    continue

                # Найти совпадения с помощью KMP поиска
                matches = kmp_search.kmp_search(
                    first_and_capital_letters.lower(), abbreviation.lower()
                )

                for match in matches:
                    normalize_match = self._normalize_match(abbreviation, match, group)

                    if normalize_match == -1:
                        continue

                    sub_group = group[
                                normalize_match: normalize_match + len(abbreviation)
                                ]
                    if (
                            abbreviation.lower()
                            != WordList.get_first_and_capital_letters_from_wordlist(
                        sub_group
                    ).lower()
                    ):
                        continue

                    yield abbreviation, sub_group