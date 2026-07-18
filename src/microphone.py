from redis.asyncio import Redis
from typing import Dict, Optional, Tuple, Any, cast
import pyaudio
import asyncio
import logging

from src.config import settings

logger = logging.getLogger(__name__)

class Microphone:
    def __init__(self, redis_provider: Redis, pyaudio_instance: pyaudio.PyAudio, loop: asyncio.AbstractEventLoop, stream_name: str) -> None:
        self.redis_provider = redis_provider
        self.pyaudio_instance = pyaudio_instance
        self.loop = loop
        self.stream_name = stream_name

        # Internal queue to hold audio chunks
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()

        self.mic_stream = self.pyaudio_instance.open(
            format=pyaudio.paInt16,
            channels=settings.MICROPHONE.CHANNELS,
            rate=settings.MICROPHONE.CAPTURE_RATE,
            input=True,
            frames_per_buffer=settings.MICROPHONE.CHUNK_SIZE,
            stream_callback=cast(Any, self._audio_callback),
        )
    
    def _audio_callback(
            self,
            in_data: Optional[bytes], 
            frame_count: int, 
            time_info: Dict[str, float], 
            status: int
        ) -> Tuple[Optional[bytes], int]:

        self.loop.call_soon_threadsafe(self.queue.put_nowait, cast(Any, in_data))
        return (None, pyaudio.paContinue) 

    async def broadcast_chunk(self, chunk_bytes: bytes):
        payload = {
            "audio_data":chunk_bytes
        }
        await self.redis_provider.xadd(
            self.stream_name,
            cast(Any, payload),
            maxlen=settings.REDIS.MAX_STREAM_LEN,
            approximate=True
        )

    async def run(self):
        self.mic_stream.start_stream()

        while self.mic_stream.is_active():
            chunk = await self.queue.get()
            try:
                await self.broadcast_chunk(chunk)
            except Exception as e:
                logger.error(f"Error occured with Microphone chunk broadcast: {e}")
            finally:
                self.queue.task_done()

    async def close(self):
        await self.redis_provider.close()
        self.mic_stream.stop_stream()
        self.mic_stream.close()
        self.pyaudio_instance.terminate()

