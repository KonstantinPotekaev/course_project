from typing import Dict, TypeVar, Tuple

from extractor_service.resource_models.base_resource_model import BaseProxyModel, BaseResourceModel


ProxyModel = TypeVar("ProxyModel", bound=BaseProxyModel)


class ResourceManager:

    def __init__(self):
        self._resource_models: Dict[str, BaseResourceModel] = {}

    def get_resource(self, name: str) -> ProxyModel:
        """ Зарегистрировать ресурс

        :param name: название ресурса
        :return: прокси ресурс
        """
        if name not in self._resource_models:
            raise ValueError(f"No resource registered with name '{name}'")
        return self._resource_models[name].proxy

    def register(self, name, resource_model: BaseResourceModel):
        """ Зарегистрировать ресурс """
        if name in self._resource_models:
            raise ValueError(f"Resource with name '{name}' already registered")
        self._resource_models[name] = resource_model

    def register_resources(self, *resources: Tuple[Tuple[str, BaseResourceModel]]):
        for resource in resources:
            self.register(*resource)

    def start(self):
        for model in self._resource_models.values():
            model.start()

    def stop(self):
        for model in self._resource_models.values():
            model.stop()

    def unlink(self, name):
        if name not in self._resource_models:
            return
        self._resource_models[name].unlink()
