from __future__ import annotations

import asyncio
import inspect
import typing
from collections import OrderedDict
from functools import partial
from inspect import getfullargspec, isasyncgenfunction, isasyncgen, iscoroutinefunction
from itertools import chain
from typing import (
    Callable,
    List,
    TypeVar,
    Type,
    Dict,
    Tuple,
    Any,
    Union,
    Optional,
    Generator,
    AsyncGenerator
)

from aioitertools import tee
from utils.aes_utils.common import async_grouper
from utils.aes_utils.models.base_model import BaseModel
from utils.status import StatusCodes

import extractor_service.common.globals as aes_globals
from extractor_service.common.struct.model.common import ExtendedBaseData, Status
from extractor_service.common.struct.model.common import BaseData

InType = TypeVar('InType', bound=BaseModel)
OutType = TypeVar('OutType', bound=BaseModel)

Transformable = Union[BaseModel, dict]
TransformerType = Dict[Tuple[Type[InType], Type[OutType]], Callable[[InType], OutType]]
AnyTransformerType = Dict[Type[OutType], Callable[[Dict], OutType]]


ITEMS_META_KEY = "_items_meta"


def _dict_get_with_raise(obj: dict, key: str) -> Any:
    if key not in obj:
        raise AttributeError(f"Key '{key}' not found in dict")
    return obj[key]


class StepProcessingMixin:

    def __init__(self, transformer: BaseDataTransformer):
        self._transformer = transformer

    @staticmethod
    def _get_func_args(func: Callable) -> Dict[str, bool]:
        arg_spec = getfullargspec(func)
        args = arg_spec.args
        default_count = 0 if arg_spec.defaults is None else len(arg_spec.defaults)
        required_arg_count = len(args) - default_count
        if args and args[0] == "self":
            args.remove('self')
            required_arg_count -= 1

        pos_args = {arg_name: (arg_idx < required_arg_count)
                    for arg_idx, arg_name in enumerate(args)}

        kw_defaults = arg_spec.kwonlydefaults or {}
        kw_args = {arg_name: (arg_name in kw_defaults)
                   for arg_name in arg_spec.kwonlyargs}
        return dict(**pos_args, **kw_args)

    @staticmethod
    def _map_attribute(attr_name: str, mapping: Dict[str, str]) -> str:
        return mapping[attr_name] if attr_name in mapping else attr_name

    def _get_attributes(self,
                        result_item: Union[Dict, BaseModel],
                        attrs: Dict[str, bool],
                        meta: Optional[Dict[str, Any]] = None,
                        attr_mapping: Optional[Dict[str, str]] = None) -> dict:
        if isinstance(result_item, dict):
            getter = partial(_dict_get_with_raise, result_item)
        elif isinstance(result_item, BaseModel):
            getter = partial(getattr, result_item)
        else:
            raise ValueError("Result item must be of type 'dict' or 'pydantic.BaseModel'")

        result_attributes = {}
        for arg_name, required in attrs.items():
            mapped_arg_name = arg_name
            if attr_mapping:
                mapped_arg_name = self._map_attribute(arg_name, attr_mapping)

            try:
                result_attributes[arg_name] = getter(mapped_arg_name)
            except AttributeError:
                if meta and arg_name in meta:
                    result_attributes[arg_name] = meta[mapped_arg_name]
                    continue

                if not required:
                    continue
        return result_attributes

    @staticmethod
    def _merge_result_parts(parts: Tuple[Union[Exception, List[BaseData]]]) -> Optional[BaseData]:
        if not parts:
            return None

        if not parts or not parts[0]:
            return None

        merged_item = parts[0]
        if isinstance(merged_item, Exception):
            aes_globals.service_logger.exception("Error [parse result]", exc_info=merged_item)
            return None

        merged_item = merged_item[0]
        for item in parts[1:]:
            if isinstance(merged_item, Exception):
                aes_globals.service_logger.exception("Error [parse result]", exc_info=merged_item)
                return None

            # если встречаем ошибку, то сразу выходим,
            # т.к. в этом случае объект считается бракованным
            if merged_item.status.code != StatusCodes.OK.code:
                return merged_item

            merged_item = merged_item.merge(item[0])
        return merged_item

    @staticmethod
    def get_fields_as_dict(item: BaseModel) -> Dict[str, Any]:
        return {f_name: getattr(item, f_name) for f_name in item.__fields__}

    def _update_item_meta(self, item: BaseData, meta: Dict[str, Any]) -> Dict[str, Any]:
        item_meta = meta[ITEMS_META_KEY].get(item.key_, {})

        item_field_dict = self.get_fields_as_dict(item)
        item_meta.update(item_field_dict)
        meta[ITEMS_META_KEY][item.key_] = item_meta
        return item_meta

    @staticmethod
    async def _async_grouper(item_gen: AsyncGenerator[BaseData, None], batch_size: int) -> AsyncGenerator:
        cur_batch = []
        async for item in item_gen:
            # объекты, на которых на предыдущем шаге возникла ошибка
            # не отправляем на следующие шаги
            if item.status.code != StatusCodes.OK.code:
                continue

            cur_batch.append(item)
            if len(cur_batch) >= batch_size:
                yield cur_batch
                cur_batch = []

        if cur_batch:
            yield cur_batch

    @staticmethod
    async def _non_broken_gen(item_gen: AsyncGenerator[BaseData, None]) -> AsyncGenerator:
        async for item in item_gen:
            # объекты, на которых на предыдущем шаге возникла ошибка
            # не отправляем на следующие шаги
            if item.status.code != StatusCodes.OK.code:
                continue

            yield item

    async def _transform_gen(self,
                             data_gen: Union[AsyncGenerator[BaseData, None],
                                             Generator[BaseData, None]],
                             pre_collection_step: PipelineStep):
        async for item_batch in async_grouper(data_gen, pre_collection_step.pre_collected_batch_size):
            for item in self._transformer.transform_list(item_batch, pre_collection_step.in_item_type):
                yield item

    async def _enrich_with_meta(self,
                                data: AsyncGenerator[BaseData, None],
                                meta: Optional[Dict[str, Any]]) -> AsyncGenerator[BaseData, None]:
        async for item in data:
            item_meta = self._update_item_meta(item, meta)
            yield ExtendedBaseData.construct(**item_meta)

    async def _run_dependent_steps(self,
                                   data: AsyncGenerator[BaseData, None],
                                   meta: Optional[Dict[str, Any]] = None,
                                   steps: List[PipelineStep] = None,
                                   steps_with_pre_collection: List[PipelineStep] = None,
                                   attr_mapping: Dict[str, str] = None) -> List[BaseData]:
        pre_collected_coro_batch = []

        data = self._enrich_with_meta(data, meta)
        if steps_with_pre_collection:
            data_gens = tee(data, n=(len(steps_with_pre_collection)+1))
            for idx, step in enumerate(steps_with_pre_collection):
                attrs = self._get_func_args(step.processor)
                attr_dict = self._get_attributes(meta, attrs, attr_mapping=attr_mapping)

                non_broken_data = self._non_broken_gen(data_gens[idx + 1])
                pre_collected_coro_batch.append(
                    asyncio.create_task(step.process(non_broken_data, meta=meta, **attr_dict))
                )
            data = data_gens[0]

        item_task_batches = []
        collected_items = OrderedDict()
        broken_items = []
        async for result_item in data:
            # объекты, на которых на предыдущем шаге возникла ошибка
            # не отправляем на следующие шаги
            if result_item.status.code != StatusCodes.OK.code:
                broken_items.append(result_item)
                continue

            collected_items[result_item.key_] = result_item

            item_coro_batch = []
            for step in steps:
                attrs = self._get_func_args(step.processor)
                attr_dict = self._get_attributes(result_item=result_item,
                                                 attrs=attrs,
                                                 meta=meta,
                                                 attr_mapping=attr_mapping)
                item_coro_batch.append(
                    asyncio.create_task(step.process(meta=meta, **attr_dict))
                )

            if item_coro_batch:
                item_task_batches.append(item_coro_batch)

        if not (steps or steps_with_pre_collection):
            return list(chain(collected_items.values(), broken_items))

        if not collected_items:
            return list(chain(collected_items.values(), broken_items))

        # собираем результаты
        item_keys = list(collected_items.keys())
        for key, coro_batch in zip(item_keys, item_task_batches):
            res_item_parts = await asyncio.gather(*coro_batch, return_exceptions=True)
            item = self._merge_result_parts(res_item_parts)  # noqa
            if item is None:
                collected_items[key].status = Status.make_status(status=StatusCodes.INTERNAL_ERROR)
                broken_items.append(collected_items[key])
                del collected_items[key]
                continue
            collected_items[key] = collected_items[key].merge(item)

        pre_collected_steps_results = await asyncio.gather(*pre_collected_coro_batch,
                                                           return_exceptions=True)

        for result in pre_collected_steps_results:
            if isinstance(result, Exception):
                aes_globals.service_logger.exception("Error [parse precollected result]", exc_info=result)

                for key, item in collected_items.items():
                    item.status = Status.make_status(status=StatusCodes.INTERNAL_ERROR)
                return list(chain(collected_items.values(), broken_items))

        for item in chain(*pre_collected_steps_results):
            key = item.key_

            if collected_items[key].status.code != StatusCodes.OK.code:
                continue

            collected_items[key] = collected_items[key].merge(item)
        return list(chain(collected_items.values(), broken_items))


class BaseDataTransformer:

    def __init__(self):
        self._transformers: TransformerType = {}
        self._from_any_transformers: AnyTransformerType = {}

    @staticmethod
    def _default_transformer(data: Union[Transformable, BaseModel], out_type: Type[Transformable]) -> OutType:
        if isinstance(data, out_type):
            return data

        if isinstance(data, BaseModel):
            data = data.dict()
        return out_type.construct(**data)

    def transform(self, data: InType, out_type: Type[OutType]) -> OutType:
        in_type_str = str(type(data))
        out_type_str = str(out_type)
        transformer_key = (in_type_str, out_type_str)
        if transformer_key in self._transformers:
            transformer = self._transformers[transformer_key]
            return transformer(data)

        if out_type_str in self._from_any_transformers:
            transformer = self._from_any_transformers[out_type_str]
            return transformer(data)
        return self._default_transformer(data, out_type)

    def transform_gen(self, data_items: typing.Iterable[BaseModel], item_type: Type[BaseModel]) -> Generator[BaseModel]:
        if not data_items:
            return

        for item in data_items:
            yield self.transform(item, item_type)

    def transform_list(self, data_items: typing.Iterable[Transformable],
                       item_type: Type[OutType]) -> List[OutType]:
        if not data_items:
            return []

        return list(self.transform_gen(data_items, item_type))

    def register(self, in_type: Any, out_type: Any, transformer: Callable):
        if in_type == typing.Any:
            self._from_any_transformers[str(out_type)] = transformer
            return

        self._transformers[(str(in_type), str(out_type))] = transformer


class PipelineStep(StepProcessingMixin):

    def __init__(self,
                 processor: Callable,
                 pre_collected: bool = False,
                 pre_collected_batch_size: Optional[int] = -1,
                 transformer: Optional[BaseDataTransformer] = BaseDataTransformer(),
                 attr_mapping: Optional[dict] = None,
                 in_item_type: Optional[Type[BaseData]] = None):
        super().__init__(transformer)

        if pre_collected and in_item_type is None:
            raise ValueError("Input item type must be defined for 'pre_collected' steps")

        self._processor_func = processor
        self._pre_collected = pre_collected
        self._pre_collected_batch_size = pre_collected_batch_size
        self._attr_mapping = attr_mapping
        self._in_item_type = in_item_type

        self._steps: List[PipelineStep] = []
        self._steps_with_pre_collection: List[PipelineStep] = []

    @property
    def processor(self):
        return self._processor_func

    @property
    def pre_collected(self):
        return self._pre_collected

    @property
    def pre_collected_batch_size(self):
        return self._pre_collected_batch_size

    @property
    def steps(self):
        return self._steps

    @property
    def pre_collected_steps(self):
        return self._steps_with_pre_collection

    @property
    def in_item_type(self):
        return self._in_item_type

    @staticmethod
    async def _make_async_gen(obj: Union[List, Generator, AsyncGenerator, BaseModel, Dict]) -> Any:
        if isasyncgen(obj):
            async for item in obj:
                yield item
            return

        if isinstance(obj, (BaseModel, dict)):
            yield obj
            return

        for item in obj:
            yield item

    async def _run_processor_func(self, *args, **kwargs) -> AsyncGenerator:
        if isasyncgenfunction(self._processor_func):
            results = self._processor_func(*args, **kwargs)
        elif iscoroutinefunction(self._processor_func):
            results = await self._processor_func(*args, **kwargs)
        else:
            results = self._processor_func(*args, **kwargs)

        # преобразуем в асинхронный генератор
        return self._make_async_gen(results)

    async def _run_processor_in_batch(self, data: AsyncGenerator, *args, **kwargs) -> AsyncGenerator:
        tasks = []
        data_batches_iter = async_grouper(data, self._pre_collected_batch_size).__aiter__()
        cur_future = asyncio.ensure_future(data_batches_iter.__anext__())
        while True:
            try:
                done, _ = await asyncio.wait([cur_future], timeout=0.05)
                if not done:
                    raise asyncio.TimeoutError()

                data_batch = list(done)[0].result()
                task = asyncio.create_task(self._run_processor_func(data_batch, *args[1:], **kwargs))
                tasks.append(task)
                cur_future = asyncio.ensure_future(data_batches_iter.__anext__())
            except (asyncio.TimeoutError, StopAsyncIteration):
                pass

            if not tasks:
                if cur_future.done():
                    break
                continue

            # проверяем готовые результаты
            done_tasks, pending_tasks = await asyncio.wait(tasks, timeout=0.05)
            for finished_task in done_tasks:
                batch_results = await finished_task
                async for item in batch_results:
                    yield item

            tasks = list(pending_tasks)

    async def process(self,
                      *args,
                      meta: Optional[Dict[str, Any]] = None,
                      **kwargs) -> List[BaseData]:
        if self._pre_collected:
            data = args[0]
            if not args or not (isinstance(data, list) or inspect.isasyncgen(data)):
                raise ValueError("Data must be asyncgen for pre-collected step")

            data = self._make_async_gen(data)

            data = self._transform_gen(data, self)
            results = self._run_processor_in_batch(data, *args, **kwargs)
        else:
            results = await self._run_processor_func(*args, **kwargs)

        return await self._run_dependent_steps(
            data=results,
            meta=meta,
            steps=self._steps,
            steps_with_pre_collection=self._steps_with_pre_collection,
            attr_mapping=self._attr_mapping
        )

    def add_next_step(self, step: PipelineStep) -> PipelineStep:
        if step._pre_collected:
            self._steps_with_pre_collection.append(step)
        else:
            self._steps.append(step)
        return step

    def add_branch(self, *steps: PipelineStep) -> PipelineStep:
        if not steps:
            return self

        cur_step = self.add_next_step(steps[0])
        for step in steps[1:]:
            cur_step = cur_step.add_next_step(step)
        return cur_step


class Pipeline(StepProcessingMixin):
    def __init__(self,
                 initial_step: PipelineStep,
                 in_item_type: Type[BaseData] = None,
                 out_item_type: Type[OutType] = None,
                 transformer: BaseDataTransformer = BaseDataTransformer()):
        super().__init__(transformer)

        self._initial_step = initial_step
        self._in_item_type = in_item_type
        self._out_item_type = out_item_type

    @staticmethod
    def _merge_with_results(data_items: List[BaseData],
                            results: List[BaseData]) -> List[BaseData]:
        key_to_result = {item.key_: item for item in results}
        new_items = []
        for item in data_items:
            if item.key_ not in key_to_result:
                item.status = Status.make_status(status=StatusCodes.INTERNAL_ERROR)
                continue
            new_items.append(
                item.merge(key_to_result[item.key_])
            )
        return new_items

    def _init_meta(self, meta: Dict[str, Any], data_items: List[BaseData]) -> Dict[str, Any]:
        data_items_dict = {item.key_: self.get_fields_as_dict(item)
                           for item in data_items}

        if meta is None:
            meta = {ITEMS_META_KEY: data_items_dict}

        if ITEMS_META_KEY not in meta:
            meta[ITEMS_META_KEY] = data_items_dict
        return meta

    async def start(self, data_items: List[BaseModel], meta: Dict[str, Any] = None) -> List[OutType]:
        if self._in_item_type is not None:
            data_items = self._transformer.transform_list(data_items, self._in_item_type)

        meta = self._init_meta(meta, data_items)
        attrs = self._get_func_args(self._initial_step.processor)
        attr_dict = self._get_attributes(meta, attrs)
        results = await self._initial_step.process(data_items, meta=meta, **attr_dict)

        if self._out_item_type is not None:
            results = self._transformer.transform_list(results, self._out_item_type)
        return results

    def add_next_step(self, step: PipelineStep) -> PipelineStep:
        return self._initial_step.add_next_step(step)

    def add_branch(self, *steps: PipelineStep) -> PipelineStep:
        return self._initial_step.add_branch(*steps)
