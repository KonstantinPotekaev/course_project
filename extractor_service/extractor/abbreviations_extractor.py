from time import time
from typing import Dict

import extractor_service.common.globals as aes_globals
from extractor_service.common.models.abbreviation_extractor import AbbreviationExtractorResultsData, \
    AbbreviationExtractorRequestMsg, AbbreviationExtractorResponseMsg
from extractor_service.common.struct.language import LanguageEnum
from extractor_service.common.struct.word import WordList
from extractor_service.extractor.languages.language_facture import get_language_instance


class AbbreviationsExtractor:
    def __init__(self, language: str):
        self._logger = aes_globals.service_logger.getChild(self.__class__.__name__)
        self._language = get_language_instance(LanguageEnum(language))

    def extract(self, task_data: AbbreviationExtractorRequestMsg) -> AbbreviationExtractorResponseMsg:
        self._logger.info(f"Msg: {task_data}")
        t0 = time()

        abbreviations_dict = self._find_abbreviations_in_text(task_data.data.text)
        expansions = self._find_expansions(abbreviations_dict, task_data.data.text)

        self._logger.info(f"Done ({(time() - t0):.2f} s)")
        return AbbreviationExtractorResponseMsg(data=AbbreviationExtractorResultsData(expansions=expansions))

    def _find_expansions(self,
                         abbreviations_dict: Dict[str, Dict[str, int]],
                         text: str) -> Dict[str, Dict[str, int]]:
        # Отсортируем по длине
        abbreviations = sorted(abbreviations_dict.keys(), key=lambda x: len(x))

        # Собираем все слова из указанных файлов
        words = self._language.get_words_from_string(text)

        # Получаем "группы слов"
        word_groups = self._language.get_word_groups_from_wordlist(words)
        word_groups = self._language.remove_single_length_groups(word_groups)

        # Для подсчёта частоты
        word_list = [" ".join(g).lower() for g in word_groups]

        # Используем генератор
        expansions_gen = self._language.find_expansion(abbreviations, word_groups, word_list)
        for abbr, expansion_words in expansions_gen:
            expansion_str = " ".join(self._language.normalize_words_form(expansion_words))
            freq = WordList.count_phrase_frequency(" ".join(expansion_words).lower(), word_list)
            self._save_expansion_in_dict(abbreviations_dict, abbr, expansion_str, freq)

        return abbreviations_dict

    @staticmethod
    def _save_expansion_in_dict(abbreviations_dict: Dict[str, Dict[str, int]],
                                abbr: str,
                                expansion: str,
                                freq: int):
        abbreviations_dict[abbr].setdefault(expansion, 0)
        abbreviations_dict[abbr][expansion] += freq
