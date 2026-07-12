import redis
import pyaudio
import threading
from time import sleep
import sys
import soxr
import numpy as np

from src.config import settings 

class LiveAudioBroadcaster:
    def __init__(self, redis_provider, stream_name, pyaudio_instance):
        self.redis_provider = redis_provider
        self.stream_name = stream_name
        self.stream = pyaudio_instance.open(
            format=pyaudio.paInt16,
            channels=1,
            rate=settings.MICROPHONE.CAPTURE_RATE,
            input=True,
            frames_per_buffer=settings.MICROPHONE.CHUNK_SIZE
        ) 

    def broadcast_chunk(self, chunk_bytes):
        payload = {
            "audio_data":chunk_bytes
        }
        self.redis_provider.xadd(
            self.stream_name,
            payload,
            maxlen=settings.REDIS.MAX_STREAM_LEN,
            approximate=True
        )
    
    def run(self):
        while True:
            chunk = self.stream.read(settings.MICROPHONE.CHUNK_SIZE, exception_on_overflow=False)
            self.broadcast_chunk(chunk)
    
    def close(self):
        try:
            self.stream.stop_stream()
            self.stream.close()
        except Exception:
            pass

class AudioPlayback:
    def __init__(self, redis_provider, stream_name, pyaudio_instance):
        self.redis_provider = redis_provider
        self.stream_name = stream_name
        self.stream = pyaudio_instance.open(
            format=pyaudio.paInt16, 
            channels=1, 
            rate=settings.SPEAKER.RATE,
            output=True,
            frames_per_buffer=settings.SPEAKER.CHUNK_SIZE
        )

    def run(self):
        last_id = "$"
        while True:
            response = self.redis_provider.xread({self.stream_name: last_id}, count=5, block=2_000)
            for _, messages in response:
                for message_id, payload in messages:
                    last_id = message_id
                    speaker_bytes = payload[b"audio_data"]

                    incoming_rate = int(payload.get(b"sample_rate", settings.SPEAKER.RATE))

                    if incoming_rate == settings.SPEAKER.RATE:
                        self.stream.write(speaker_bytes)
                        continue

                    audio_array = np.frombuffer(speaker_bytes, dtype=np.int16)
                    resampled_array = soxr.resample(
                        audio_array, 
                        incoming_rate, 
                        settings.SPEAKER.RATE, 
                        quality='QQ'
                    )

                    output_bytes = resampled_array.astype(np.int16).tobytes()
                    self.stream.write(output_bytes)
    
    def close(self):
        try:
            self.stream.stop_stream()
            self.stream.close()
        except Exception:
            pass


def main():
    redis_password = (
        settings.REDIS.PASSWORD.get_secret_value() if settings.REDIS.PASSWORD else None
    )
    redis_provider = redis.Redis(host=settings.REDIS.HOST, port=settings.REDIS.PORT, password=redis_password)
    pyaudio_instance = pyaudio.PyAudio()
    live_audio_broadcaster = LiveAudioBroadcaster(redis_provider, settings.STREAM_NAME.LIVE_AUDIO, pyaudio_instance)
    audio_playback = AudioPlayback(redis_provider, settings.STREAM_NAME.PLAYBACK, pyaudio_instance)

    broadcaster_thread = threading.Thread(target=live_audio_broadcaster.run, name="MicThread", daemon=True)
    playback_thread = threading.Thread(target=audio_playback.run, name="SpeakerThread", daemon=True)

    broadcaster_thread.start()
    playback_thread.start()

    try:
        while True:
            sleep(1)

            if not broadcaster_thread.is_alive():
                raise RuntimeError("Broadcaster thread crashed or exited unexpectedly.")
            if not playback_thread.is_alive():
                raise RuntimeError("Playback thread crashed or exited unexpectedly.")
    except KeyboardInterrupt:
        print("\n[System] Interrupted by user. Cleaning up assets...")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] Pipeline failure detected: {e}", file=sys.stderr)
    finally:
        print("[System] Releasing system hardware resource allocations...")
        if live_audio_broadcaster:
            live_audio_broadcaster.close()
        if audio_playback:
            audio_playback.close()
            
        pyaudio_instance.terminate()

if __name__ == "__main__":
    main()
