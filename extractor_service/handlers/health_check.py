# from time import time
#
# from prediction_utils.das_utils.das_msg import DasMsg
# from prediction_utils.das_utils.models.health_check import HealthCheckRequestMsg, HealthCheckResponseMsg, \
#     HealthCheckResponseData
# from prediction_utils.das_utils.nats.client import NatsClient
#
# import extractor_service.common.globals as aes_globals
# from extractor_service.common.env.general import NODE_LABEL
# from extractor_service.handlers.common import catch_internal_errors
#
#
# class HealthCheckHandler:
#     """ Хэндлер для обработки health-check запросов """
#
#     def __init__(self, reply_client: NatsClient):
#         self._logger = aes_globals.service_logger.getChild('handlers.health')
#         self._reply_client = reply_client
#
#     @catch_internal_errors
#     async def __call__(self, msg: DasMsg):
#         msg_data = msg.get_body_in_format(HealthCheckRequestMsg).data
#         self._logger.debug(f"Msg: {msg_data}")
#         t0 = time()
#
#         hc_response_msg = HealthCheckResponseMsg(
#             Data=HealthCheckResponseData(ResponderNode=NODE_LABEL)
#         )
#
#         await self._reply_client.ack_message(msg, hc_response_msg)
#         self._logger.debug(f"Done ({(time() - t0):.2f} s)")
#
#     async def func(self):
#         await self()
