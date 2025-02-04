import string
from typing import List


class Word:
    NONEXISTENT_WORD = " NONEXISTENTWORD123 "

    @staticmethod
    def is_english_word(word: str) -> bool:
        """ Проверяет, является ли слово английским и начинается ли оно с заглавной буквы.

        :param word: Слово для проверки.
        :return: True, если слово является английским, иначе False.
        """

        return (
            len(word) > 0
            and word[0] in string.ascii_uppercase
            and all(char in string.ascii_letters for char in word)
            and any(char in string.ascii_lowercase for char in word)
        )

    @classmethod
    def is_valid_word(cls, word: str) -> bool:
        """ Проверяет, является ли слово допустимым.

        :param word: Слово для проверки.
        :return: True, если слово является допустимым, иначе False.
        """

        return word != cls.NONEXISTENT_WORD

    @staticmethod
    def get_uppercase_letters(word: str) -> str:
        """ Находит все заглавные буквы в слове.

        :param word: Входное слово, в котором необходимо найти заглавные буквы.
        :return: Строка, содержащая все заглавные буквы из входного слова.
        """
        return "".join([char for char in word if char.isupper()])

    @staticmethod
    def get_first_letters(word: str):
        """ Извлекает первую букву в слове.

        :param word: Слово, из которого необходимо извлечь первую букву.
        :return: Строка с первой буквой.
        """
        return str(word[0])

    @staticmethod
    def get_first_and_capital_letters(word: str):
        """ Извлекает первую и заглавные буквы из слов.

        :param word: Слово, из которого необходимо извлечь первую и заглавные буквы.
        :return: Строка с первой и заглавными буквами.
        """
        first_and_capital_letters = ""

        if not word[0].isupper():
            first_and_capital_letters += word[0]
        first_and_capital_letters += Word.get_uppercase_letters(word)

        return first_and_capital_letters


class WordList(Word):
    @staticmethod
    def get_uppercase_letters_from_wordlist(wordlist: List[str]) -> str:
        """ Находит все заглавные буквы в списке слов.

        :param wordlist: Список слов, в которых нужно найти заглавные буквы.
        :return: Строка, содержащая все заглавные буквы из входного списка слов.
        """
        uppercase_letters = ""
        for word in wordlist:
            uppercase_letters += Word.get_uppercase_letters(word)

        return uppercase_letters

    @staticmethod
    def get_first_letters_from_wordlist(wordlist: List[str]) -> str:
        """ Извлекает первые буквы из списка слов.

        :param wordlist: Список слов, из которых необходимо извлечь первые и заглавные буквы.
        :return: Строка с первыми буквами.
        """
        first_letters = ""
        for word in wordlist:
            first_letters += Word.get_first_letters(word)

        return first_letters

    @staticmethod
    def get_first_and_capital_letters_from_wordlist(wordlist: List[str]) -> str:
        """ Извлекает первые и заглавные буквы из списка слов.

        :param wordlist: Список слов, из которых необходимо извлечь первые и заглавные буквы.
        :return: Строка с первыми и заглавными буквами.
        """

        first_and_capital_letters = ""
        for word in wordlist:
            first_and_capital_letters += Word.get_first_and_capital_letters(word)

        return first_and_capital_letters

    @staticmethod
    def count_phrase_frequency(phrase: str, wordlist: List[str]) -> int:
        """ Подсчитывает частоту появления фразы в wordlist.

        :param phrase: Фраза, частоту появления которой нужно посчитать.
        :param wordlist: Список слов.
        :return: Количество появлений фразы в списке слов.
        """
        return " ".join(wordlist).count(phrase)
