import redis
import pyaudio
import os
import threading
from time import sleep
import sys

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
# TODO change these stream/variable names to things more clear
LIVE_AUDIO_STREAM_NAME = os.environ.get("LIVE_AUDIO_STREAM_NAME", "live_audio_broadcast")
PLAYBACK_STREAM_NAME = os.environ.get("PLAYBACK_STREAM_NAME", "playback")

MICROPHONE_CAPTURE_RATE = 16_000
MICROPHONE_CHUNK_SIZE = 1_280 

PLAYBACK_RATE = 16_000
PLAYBACK_CHUNK_SIZE = 1_280

MAX_STREAM_LEN = 2_000

class LiveAudioBroadcaster:
    def __init__(self, redis_provider, stream_name, pyaudio_instance):
        self.redis_provider = redis_provider
        self.stream_name = stream_name
        self.stream = pyaudio_instance.open(
            format=pyaudio.paInt16,
            channels=1,
            rate=MICROPHONE_CAPTURE_RATE,
            input=True,
            frames_per_buffer=MICROPHONE_CHUNK_SIZE
        ) 

    def broadcast_chunk(self, chunk_bytes):
        payload = {
            "audio_data":chunk_bytes
        }
        self.redis_provider.xadd(
            self.stream_name,
            payload,
            maxlen=MAX_STREAM_LEN,
            approximate=True
        )
    
    def run(self):
        while True:
            chunk = self.stream.read(MICROPHONE_CHUNK_SIZE, exception_on_overflow=False)
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
            rate=PLAYBACK_RATE,
            output=True,
            frames_per_buffer=PLAYBACK_CHUNK_SIZE
        )

    def run(self):
        last_id = "$"
        while True:
            response = self.redis_provider.xread({self.stream_name: last_id}, count=5, block=0)
            for _, messages in response:
                for message_id, payload in messages:
                    last_id = message_id
                    speaker_bytes = payload[b"audio_data"]

                    self.stream.write(speaker_bytes)
    
    def close(self):
        try:
            self.stream.stop_stream()
            self.stream.close()
        except Exception:
            pass


def main():
    redis_provider = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
    pyaudio_instance = pyaudio.PyAudio()
    live_audio_broadcaster = LiveAudioBroadcaster(redis_provider, LIVE_AUDIO_STREAM_NAME, pyaudio_instance)
    audio_playback = AudioPlayback(redis_provider, PLAYBACK_STREAM_NAME, pyaudio_instance)

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
