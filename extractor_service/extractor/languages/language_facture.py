from extractor_service.common.struct.language import Language, LanguageEnum
from extractor_service.extractor.languages.english_language import English
from extractor_service.extractor.languages.russian_language import Russian


class EnglishFactory:
    @staticmethod
    def create_language() -> Language:
        return English()


class RussianFactory:
    @staticmethod
    def create_language() -> Language:
        return Russian()


class LanguageFactoryContainer:
    factories = {
        LanguageEnum.RUSSIAN.value: RussianFactory,
        LanguageEnum.ENGLISH.value: EnglishFactory,
    }

    @classmethod
    def get_factory(cls, lang_enum):
        return cls.factories.get(lang_enum.value)


def get_language_instance(lang_enum: LanguageEnum) -> Language:
    factory_class = LanguageFactoryContainer.get_factory(lang_enum)
    if not factory_class:
        raise ValueError(f'Language factory for {lang_enum.value} not found')
    return factory_class().create_language()
