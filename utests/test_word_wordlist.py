import pytest
import string
from extractor_service.common.struct.word import Word, WordList

NONEXISTENT = " NONEXISTENTWORD123 "


class TestWord:
    def test_is_english_word_true(self):
        assert Word.is_english_word("Hello") is True

    def test_is_english_word_false_no_lowercase(self):
        assert Word.is_english_word("HELLO") is False

    def test_is_english_word_false_first_not_upper(self):
        assert Word.is_english_word("hello") is False

    def test_is_english_word_empty(self):
        assert Word.is_english_word("") is False

    def test_is_valid_word_true(self):
        assert Word.is_valid_word("test") is True

    def test_is_valid_word_false(self):
        assert Word.is_valid_word(NONEXISTENT) is False

    def test_get_uppercase_letters(self):
        assert Word.get_uppercase_letters("HelloWorld") == "HW"

        assert Word.get_uppercase_letters("abc") == ""

        assert Word.get_uppercase_letters("HELLO") == "HELLO"

    def test_get_first_letters(self):
        assert Word.get_first_letters("Hello") == "H"
        assert Word.get_first_letters("world") == "w"

    def test_get_first_and_capital_letters(self):
        assert Word.get_first_and_capital_letters("Hello") == "H"

        assert Word.get_first_and_capital_letters("hello") == "h"

        assert Word.get_first_and_capital_letters("hEllo") == "hE"

        assert Word.get_first_and_capital_letters("HEllo") == "HE"


class TestWordList:
    def test_get_uppercase_letters_from_wordlist(self):
        wordlist = ["Hello", "World", "test"]

        assert WordList.get_uppercase_letters_from_wordlist(wordlist) == "HW"

    def test_get_first_letters_from_wordlist(self):
        wordlist = ["Hello", "World", "test"]

        assert WordList.get_first_letters_from_wordlist(wordlist) == "HWt"

    def test_get_first_and_capital_letters_from_wordlist(self):
        wordlist = ["Hello", "world", "hEllo"]

        expected = "HwhE"
        assert WordList.get_first_and_capital_letters_from_wordlist(wordlist) == expected

    def test_count_phrase_frequency(self):
        wordlist = ["hello", "world", "hello", "test", "hello world"]

        assert WordList.count_phrase_frequency("hello", wordlist) == 3

        assert WordList.count_phrase_frequency("world hello", wordlist) == 1


if __name__ == "__main__":
    pytest.main()
