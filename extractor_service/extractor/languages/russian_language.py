import re
import string
from pathlib import Path
from typing import List, Tuple

import pymorphy2


from extractor_service.common.struct.language import Language
from extractor_service.common.struct.word import WordList, Word
from extractor_service.extractor.languages import kmp_search


class Russian(Language):
    def __init__(self):
        super().__init__()
        self._morph = pymorphy2.MorphAnalyzer()
        self._invalid_starting_pos_tags = [
            "VERB",
            "INFN",
            "PRTF",
            "PRTS",
            "GRND",
            "NUMR",
            "NPRO",
            "PRED",
            "PREP",
            "CONJ",
            "PRCL",
            "INTJ",
        ]

    @property
    def abbreviation_pattern(self) -> str:
        return r"\b[А-ЯЁ]{2,7}\b"

    @property
    def word_pattern(self) -> str:
        return r"\b[а-яА-ЯёЁ]+\b"

    def find_abbreviations(self, text: str) -> List[str]:
        """ Находит русские аббревиатуры в тексте.

        :param text: текст, в котором нужно искать аббревиатуры
        :return: итерируемый объект, содержащий русские аббревиатуры, найденные в тексте
        """
        abbreviations = re.findall(self.abbreviation_pattern, text)

        return abbreviations

    def get_words_from_file(self, file_path: Path) -> List[str]:
        """ Извлекает слова из текстового файла.

        :param file_path: Путь к текстовому файлу.
        :return: Список всех слов, извлеченных из текстового файла.
        """

        exceptable_punctuation = {"-", "_"}
        exceptable_regex = f"[{re.escape(''.join(exceptable_punctuation))}]"
        grouping_punctuation = set((string.punctuation + "«»")) - exceptable_punctuation
        grouping_regex = f"[{re.escape(''.join(grouping_punctuation))}]"

        with open(file_path, "r+", encoding="utf-8") as file:
            text = file.read()
            text = text.replace("\n", Word.NONEXISTENT_WORD)
            text = text.replace("\t", " ")

            text = re.sub(exceptable_regex, " ", text)
            text = re.sub(grouping_regex, Word.NONEXISTENT_WORD, text)
            text = re.sub(self.abbreviation_pattern, "", text)

            text = re.sub(r"\s+", " ", text).strip()

        words = text.split()

        # words = self._remove_function_words(words)

        return words

    def get_words_from_string(self, text: str) -> List[str]:
        exceptable_punctuation = {"-", "_"}
        exceptable_regex = f"[{re.escape(''.join(exceptable_punctuation))}]"
        grouping_punctuation = set((string.punctuation + "«»")) - exceptable_punctuation
        grouping_regex = f"[{re.escape(''.join(grouping_punctuation))}]"

        text = text.replace("\n", Word.NONEXISTENT_WORD).replace("\t"," ")
        text = re.sub(exceptable_regex, " ", text)
        text = re.sub(grouping_regex, Word.NONEXISTENT_WORD, text)
        text = re.sub(self.abbreviation_pattern, "", text)

        text = re.sub(r"\s+", " ", text).strip()

        return text.split()

    def get_word_groups_from_wordlist(self, words: List[str]) -> List[List[str]]:
        """ Извлекает группы слов из текстового файла.

        :param words: Список всех слов.
        :return: Группы слов.
        """

        word_groups = []
        group = []
        frequency = {}

        reg_match_lang = re.compile(self.word_pattern)

        for word in words:
            if word and reg_match_lang.match(word):
                group.append(word)
            elif not group:
                continue
            elif (
                    not reg_match_lang.match(word) or word == Word.NONEXISTENT_WORD.strip()
            ):
                group_tuple = tuple(group)
                word_groups.append(group_tuple)
                frequency[group_tuple] = frequency.get(group_tuple, 0) + 1
                group = []

        if len(group) > 1:
            group_tuple = tuple(group)
            word_groups.append(group_tuple)
            frequency[group_tuple] = frequency.get(group_tuple, 0) + 1

        optimized_word_groups = [list(group) for group in set(map(tuple, word_groups))]

        optimized_word_groups = self.remove_single_length_groups(optimized_word_groups)

        final_word_groups = []

        for i in range(len(optimized_word_groups)):
            counter = frequency.get(tuple(optimized_word_groups[i]), 1)
            optimized_word_groups[i] = self.__remove_function_words(
                optimized_word_groups[i]
            )
            final_word_groups.extend([optimized_word_groups[i]] * counter)

        return final_word_groups

    def __remove_function_words(
            self,
            words: List[str],
    ) -> List[str]:
        """ Находит функциональные слова (предлоги, союзы и прочее) и удаляет их из текста.

        :param words: Список слов.
        :return: Список слов без специальных слов.
        """
        function_word_tags = ["PRCL", "PREP", "CONJ", "INTJ"]
        clear_words = []

        for word in words:
            p = self._morph.parse(word)[0]

            if any(tag in p.tag for tag in function_word_tags):
                continue

            clear_words.append(word)

        return clear_words

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
                    p = self._morph.parse(sub_group[0])[0]
                    # Части речи, с которых не могут начинаться расшифровки.

                    if (
                            abbreviation.lower()
                            != WordList.get_first_and_capital_letters_from_wordlist(
                        sub_group
                    ).lower()
                            or any(tag in p.tag for tag in self._invalid_starting_pos_tags)
                    ):
                        continue

                    yield abbreviation, sub_group

    def normalize_words_form(self, words: List[str]) -> List[str]:
        normalized_words = []
        for word in words:
            normalized_words.append(self._morph.parse(word)[0].normal_form)
        return normalized_words