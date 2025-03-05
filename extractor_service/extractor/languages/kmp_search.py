from typing import List


def compute_prefix_function(pattern: str) -> List[int]:
    """ Вычисляет префикс-функцию для шаблона.
    :param pattern: Шаблон, для которого вычисляется префикс-функция.
    :return: Список, содержащий префикс-функцию для шаблона.
    """
    prefix_function = [0] * len(pattern)
    j = 0

    for i in range(1, len(pattern)):
        while j > 0 and pattern[i] != pattern[j]:
            j = prefix_function[j - 1]

        if pattern[i] == pattern[j]:
            j += 1
            prefix_function[i] = j
        else:
            prefix_function[i] = 0

    return prefix_function


def kmp_search(text: str, pattern: str) -> List[int]:
    """ Выполняет поиск шаблона в тексте с использованием алгоритма Кнута-Морриса-Пратта (KMP).

    :param text: Исходный текст, в котором нужно найти подстроку.
    :param pattern: Шаблон (подстрока), которую нужно найти в тексте.
    :return: Список индексов, в которых начинается шаблон в тексте.
             Если совпадений нет, возвращает пустой список.
    """
    prefix_function = compute_prefix_function(pattern)

    matches = []

    i = 0  # Индекс в тексте
    j = 0  # Индекс в шаблоне

    while i < len(text):
        if text[i] == pattern[j]:
            i += 1
            j += 1
            if j == len(pattern):
                matches.append(i - j)
                j = prefix_function[j - 1]
        else:
            if j > 0:
                j = prefix_function[j - 1]
            else:
                i += 1

    return matches
