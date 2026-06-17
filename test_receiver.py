import os
import numpy as np
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
LIVE_AUDIO_STREAM_NAME = os.environ.get("LIVE_AUDIO_STREAM_NAME", "live_audio_broadcast")

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
    last_id = "$"  # Listen only for new chunks sent after this script starts
    
    print(f"[Tester] Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    print(f"[Tester] Watching stream '{LIVE_AUDIO_STREAM_NAME}' for mic data... Speak into the mic!")
    
    while True:
        # Block indefinitely waiting for mic data chunks
        response = r.xread({LIVE_AUDIO_STREAM_NAME: last_id}, count=1, block=2_000)
        
        for _, messages in response:
            for message_id, payload in messages:
                last_id = message_id
                
                # Extract the raw binary chunk
                raw_audio = payload[b"audio_data"]
                
                # Convert the raw bytes back into numbers to check the volume level
                audio_data = np.frombuffer(raw_audio, dtype=np.int16)
                rms = np.sqrt(np.mean(audio_data.astype(np.float32)**2))
                
                # Create a simple visual volume meter in the terminal
                meter = "#" * int(rms / 200)
                print(f"[Mic Meter] {meter:<50} (RMS: {int(rms)})", end="\r")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Tester] Stopping receiver test.")