from collections.abc import Mapping
from dataclasses import fields, _FIELDS, _FIELD     # NOQA


class DataclassMappingMixin(Mapping):
    """ Миксин для dataclass, поддерживающий семантику раскрытия полей
     в формате '**object' """

    def __iter__(self):
        return (f.name for f in fields(self))   # NOQA

    def __getitem__(self, key):
        field = getattr(self, _FIELDS)[key]
        if field._field_type is not _FIELD:     # NOQA
            raise KeyError(f"'{key}' is not a dataclass field.")
        return getattr(self, field.name)

    def __len__(self):
        return len(fields(self))                # NOQA
