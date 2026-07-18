
from redis.asyncio import Redis
import asyncio
import logging
import pyaudio

from src.audio_mixer import AudioMixer, AudioMixerEventNotifier
from src.speaker import Speaker, SpeakerChannelConfig, SpreakerChannel
from src.microphone import Microphone
from src.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] (%(filename)s:%(lineno)d) %(asctime)s -> %(message)s",
)

logger = logging.getLogger(__name__)

async def main():
    redis_password = (
        settings.REDIS.PASSWORD.get_secret_value() if settings.REDIS.PASSWORD else None
    ) 
    redis_provider = Redis(host=settings.REDIS.HOST, port=settings.REDIS.PORT, password=redis_password)
    
    pyaudio_instance = pyaudio.PyAudio()

    loop = asyncio.get_running_loop()
    audio_mixer_event_notifier = AudioMixerEventNotifier(redis_provider, settings.AUDIO_MIXER.EVENT_NOTIFIER_STREAM_NAME, loop)

    audio_mixer = AudioMixer(audio_mixer_event_notifier, pyaudio_instance)

    music_channel = SpreakerChannel(
        name="music",
        stream_name="music",
        last_id="$",
    )
    agent_channel = SpreakerChannel(
        name="agent",
        stream_name="agent",
        last_id="$",
    )
    notification_channel = SpreakerChannel(
        name="notification",
        stream_name="notification",
        last_id="$",
    )

    speaker_channel_config = SpeakerChannelConfig(
        music=music_channel,
        agent=agent_channel,
        notification=notification_channel,
    )
    
    speaker = Speaker(speaker_channel_config, audio_mixer, redis_provider)
    microphone = Microphone(redis_provider, pyaudio_instance, loop, settings.MICROPHONE.STREAM_NAME)

    processes = [speaker.run(), microphone.run()]

    logger.info("Starting speaker and microphone processes")
    
    try:
        await asyncio.gather(*processes)
    except asyncio.CancelledError:
        logger.info("Gracefully stopping ingestion...")
    finally:
        await speaker.close()
        await microphone.close()
        

if __name__ == "__main__":
    asyncio.run(main())