from extractor_service.common.struct.model.abbreviation_extractor import TextContent
from extractor_service.common.struct.model.common import ContentList, LoadedContent


async def merge_contents(content_id: str, container_contents: ContentList) -> LoadedContent:
    text = "".join(f"{item}. " for item in container_contents if item).strip()
    return TextContent.construct(key_=content_id,
                                 text=text)
