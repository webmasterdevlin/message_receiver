import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

from message_bus import MessageBus

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared list to store received messages
messages = []


async def receive_messages(message_bus):
    while True:
        await message_bus.start_consuming(service_callback_handler=process_message)
        await asyncio.sleep(1)  # short delay before next poll


async def process_message(msg):
    messages.append(str(msg))
    logger.info(f"Received and completed message: {msg}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background task
    message_bus = MessageBus()
    task = asyncio.create_task(receive_messages(message_bus))
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
