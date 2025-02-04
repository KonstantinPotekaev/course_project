from typing import Dict


def merge_abbreviations_dicts(total_dict: Dict[str, Dict[str, int]],
                              new_part: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    for abbr, expansions in new_part.items():
        if abbr not in total_dict:
            total_dict[abbr] = {}
        for expansion, freq in expansions.items():
            total_dict[abbr].setdefault(expansion, 0)
            total_dict[abbr][expansion] += freq
    return total_dict
