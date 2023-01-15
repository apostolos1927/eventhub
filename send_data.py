import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=".....",
        eventhub_name="....",
    )
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()
        f = open("C://Users//....//testdata.json")
        json_temp = json.load(f)
        # Add events to the batch.
        event_data_batch.add(EventData(str(json_temp)))

        await producer.send_batch(event_data_batch)


loop = asyncio.get_event_loop()
loop.run_until_complete(run())
