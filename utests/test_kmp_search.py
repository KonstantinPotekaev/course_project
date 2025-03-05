import pytest
from extractor_service.extractor.languages.kmp_search import compute_prefix_function, kmp_search


class TestKMP:
    @pytest.mark.parametrize(
        "pattern, expected_prefix",
        [

            ("", []),

            ("abcd", [0, 0, 0, 0]),

            ("aaaa", [0, 1, 2, 3]),

            ("ababaca", [0, 0, 1, 2, 3, 0, 1]),

            ("abcabd", [0, 0, 0, 1, 2, 0]),
        ]
    )
    def test_compute_prefix_function(self, pattern, expected_prefix):
        result = compute_prefix_function(pattern)
        assert result == expected_prefix

    @pytest.mark.parametrize(
        "text, pattern, expected_matches",
        [

            ("ababcababa", "aba", [0, 5, 7]),

            ("aaaaa", "aa", [0, 1, 2, 3]),

            ("abcdef", "gh", []),

            ("", "a", []),

            ("pattern", "pattern", [0]),
        ]
    )
    def test_kmp_search(self, text, pattern, expected_matches):
        result = kmp_search(text, pattern)
        assert result == expected_matches

    def test_kmp_search_empty_pattern(self):
        with pytest.raises(IndexError):
            kmp_search("some text", "")


if __name__ == "__main__":
    pytest.main()
