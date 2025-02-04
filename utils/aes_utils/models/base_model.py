from __future__ import annotations

from typing import Any, Dict, TypeVar

from pydantic import BaseModel as PydanticBaseModel, create_model
from pydantic.fields import SHAPE_LIST, SHAPE_SINGLETON

ModelType = TypeVar("ModelType")


def to_pascal(string: str) -> str:
    return ''.join(word.capitalize() for word in string.split('_'))


class BaseModel(PydanticBaseModel):
    # Базовая модель с возможностью "быстрого" (без валидации) конструирования вложенных моделей
    # По-умолчанию, BaseModel не поддерживает возможность конструирования вложенных моделей
    # https://github.com/pydantic/pydantic/issues/1168

    class Config:
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        alias_generator = to_pascal
        by_alias = True

    @classmethod
    def construct_union(cls, union_field, value):
        for field in union_field.sub_fields:
            try:
                return cls.construct_field(field, value)
            except (AttributeError, TypeError) as ex:
                continue

        if value is None and not union_field.required:
            return union_field.get_default()
        return value

    @classmethod
    def construct_field(cls, field, value):
        if field.shape == SHAPE_SINGLETON:
            if not field.sub_fields:
                if isinstance(value, field.outer_type_):
                    return value
                return field.outer_type_.construct(**value)
            return cls.construct_union(field, value)

        if field.shape == SHAPE_LIST:
            return [
                field.type_.construct(**e)
                for e in value
            ]
        else:
            return field.outer_type_.construct(**value)

    @classmethod
    def construct_or_default(cls, field, value):
        try:
            return cls.construct_field(field, value)
        except (AttributeError, TypeError) as ex:
            if value is None and not field.required:
                return field.get_default()
            return value

    @classmethod
    def construct(cls, _fields_set=None, extra: bool = False, **values):
        new_model = cls.__new__(cls)                                        # pylint: disable=E1120
        fields_values = {}

        for name, field in cls.__fields__.items():
            name_alias = field.alias
            if name_alias in values:  # проверка для Optional полей
                fields_values[name] = cls.construct_or_default(field, values[name_alias])
            elif getattr(cls.Config, "allow_population_by_field_name", False) and name in values:
                fields_values[name] = cls.construct_or_default(field, values[name])
            elif not field.required:
                fields_values[name] = field.get_default()
            else:
                raise AttributeError(f"Required field '{name}' is not found in values")

        object.__setattr__(new_model, '__dict__', fields_values)
        if _fields_set is None:
            _fields_set = set(values.keys())

        object.__setattr__(new_model, '__fields_set__', _fields_set)
        new_model._init_private_attributes()
        return new_model

    @staticmethod
    def get_fields_as_dict(item: BaseModel) -> Dict[str, Any]:
        return {f_name: getattr(item, f_name) for f_name in item.__fields__}

    def merge(self, model: ModelType) -> ModelType:
        # TODO: merge in-place (copy - update)
        added_fields = {}
        for name, field1 in model.__fields__.items():
            if name in self.__fields__:
                continue

            model_field = model.__fields__[name]
            added_fields[name] = (model_field.annotation, model_field.field_info)

        new_model = create_model(f"Merged_{self.__repr_name__()}_{model.__repr_name__()}",
                                 __base__=type(self), **added_fields)

        values = self.get_fields_as_dict(self)
        values.update(
            {
                f_name: getattr(model, f_name)
                for f_name in added_fields
            }
        )
        return new_model.construct(**values)

    def json(self, **kwargs) -> str:
        if "by_alias" in kwargs:
            return super().json(**kwargs)

        by_alias = False
        if getattr(self.Config, "by_alias", False):
            by_alias = self.Config.by_alias
        return super().json(by_alias=by_alias, **kwargs)

    def dict(self, **kwargs) -> dict:
        if "by_alias" in kwargs:
            return super().dict(**kwargs)

        by_alias = False
        if getattr(self.Config, "by_alias", False):
            by_alias = self.Config.by_alias
        return super().dict(by_alias=by_alias, **kwargs)
