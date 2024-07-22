import os
from azure.servicebus.aio import ServiceBusClient
from fastapi import FastAPI
import asyncio
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load connection string and queue name from environment variables
CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared list to store received messages
messages = []


async def receive_messages():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)
    async with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)
        async with receiver:
            while True:
                received_msgs = await receiver.receive_messages(max_message_count=10, max_wait_time=5)
                for msg in received_msgs:
                    messages.append(str(msg))
                    await receiver.complete_message(msg)
                    logger.info(f"Received and completed message: {msg}")
                await asyncio.sleep(1)  # short delay before next poll


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background task
    task = asyncio.create_task(receive_messages())
    yield
    # Stop the background task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    # Return the received messages
    return {"messages": messages}
