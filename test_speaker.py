import os
import time
import numpy as np
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
PLAYBACK_STREAM_NAME = os.environ.get("PLAYBACK_STREAM_NAME", "playback")

RATE = 16000
CHUNK = 1280
FREQUENCY = 440  # 440Hz is a standard musical 'A' tone

def generate_sine_wave_chunk(start_sample):
    """Generates an 80ms sine wave audio chunk calculation."""
    t = (np.arange(start_sample, start_sample + CHUNK)) / RATE
    sine_wave = np.sin(2 * np.pi * FREQUENCY * t)
    
    # Scale float array to fit signed 16-bit PCM integer boundaries (-32768 to 32767)
    audio_ints = (sine_wave * 16384).astype(np.int16) 
    return audio_ints.tobytes()

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    print(f"[Tester] Pushing continuous 440Hz beep into stream '{PLAYBACK_STREAM_NAME}'...")
    print("Press Ctrl+C to stop the tone.")
    
    sample_counter = 0
    max_stream_len = 100  # Tight threshold so old unplayed audio disappears quickly
    
    # Calculate how long an 80ms chunk takes so we don't overwhelm the network
    chunk_duration = CHUNK / RATE 

    while True:
        start_time = time.time()
        
        # 1. Generate the next segment of the sound wave
        raw_audio_chunk = generate_sine_wave_chunk(sample_counter)
        sample_counter += CHUNK
        
        # 2. Push it to Redis
        payload = {"audio_data": raw_audio_chunk}
        r.xadd(PLAYBACK_STREAM_NAME, payload, maxlen=max_stream_len, approximate=True)
        
        # 3. Precision sleep regulator loop to mimic real-time streaming pace
        elapsed_time = time.time() - start_time
        sleep_time = max(0.0, chunk_duration - elapsed_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Tester] Stopping speaker test.")