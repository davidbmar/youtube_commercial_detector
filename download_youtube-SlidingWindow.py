#!/usr/bin/python3
import os
import subprocess
import requests
import time
from pytubefix import YouTube

# Define the temporary directory and ensure it exists
TEMP_DIR = "./temp"
os.makedirs(TEMP_DIR, exist_ok=True)

def download_audio(youtube_url):
    """Download audio-only stream from YouTube using pytubefix into TEMP_DIR."""
    yt = YouTube(youtube_url)
    audio_stream = yt.streams.filter(only_audio=True).first()
    audio_file = os.path.join(TEMP_DIR, "audio.mp4")
    audio_stream.download(output_path=TEMP_DIR, filename="audio.mp4")
    print(f"Downloaded audio file: {audio_file}")
    return audio_file

def convert_to_wav(input_file):
    """Convert the MP4 audio file to WAV format using ffmpeg in TEMP_DIR."""
    output_file = os.path.join(TEMP_DIR, "audio.wav")
    subprocess.run(["ffmpeg", "-y", "-i", input_file, output_file], check=True)
    print(f"Converted audio to WAV: {output_file}")
    return output_file

def get_audio_duration(wav_file):
    """Get duration (in seconds) of the WAV file using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", wav_file
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    duration = float(result.stdout.strip())
    return duration

def split_audio_sliding(wav_file, segment_duration=600, overlap=30):
    """
    Split the WAV file into overlapping sliding window segments.
    For example, with segment_duration=600 and overlap=30:
      - Segment 0:  0 to 600 sec
      - Segment 1: 570 to 1170 sec, etc.
    Each segment is saved in TEMP_DIR with names segment_000.wav, segment_001.wav, etc.
    """
    total_duration = get_audio_duration(wav_file)
    segments = []
    step = segment_duration - overlap  # e.g., 600 - 30 = 570 seconds
    idx = 0
    start_time = 0
    while start_time < total_duration:
        output_file = os.path.join(TEMP_DIR, f"segment_{idx:03d}.wav")
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(start_time),
            "-t", str(segment_duration),
            "-i", wav_file,
            output_file
        ]
        subprocess.run(cmd, check=True)
        segments.append(output_file)
        print(f"Created segment {idx} from {start_time:.0f} to {start_time + segment_duration:.0f} sec")
        idx += 1
        start_time += step
    print(f"Created {len(segments)} overlapping segments.")
    return segments

def transcribe_segment(segment_file, retries=3, backoff_factor=2):
    """Transcribe an audio segment via the local API with retry logic."""
    api_url = "http://localhost:8000/v1/audio/transcriptions"  # Update if needed
    for attempt in range(retries):
        try:
            with open(segment_file, "rb") as f:
                files = {"file": f}
                data = {"language": "en"}
                response = requests.post(api_url, files=files, data=data, timeout=300)
            if response.status_code == 200:
                result = response.json()
                return result.get("text", "")
            else:
                print(f"Segment {segment_file} failed with status {response.status_code}. Retrying...")
        except Exception as e:
            print(f"Error transcribing {segment_file}: {e}. Retrying...")
        time.sleep(backoff_factor ** attempt)
    return ""

def seconds_to_hms(seconds):
    """Convert seconds to HH:MM:SS format."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

def main():
    youtube_url = "https://www.youtube.com/watch?v=TOQtJch3kGk"

    # Step 1: Download audio and convert to WAV
    audio_mp4 = download_audio(youtube_url)
    audio_wav = convert_to_wav(audio_mp4)
    
    # Step 2: Create overlapping sliding window segments (10 min each with 30 sec overlap)
    segments = split_audio_sliding(audio_wav, segment_duration=600, overlap=30)
    
    # Step 3: Transcribe each segment and accumulate results with time markers
    final_transcription = ""
    step = 600 - 30  # Step in seconds (e.g., 570 sec)
    for idx, seg_file in enumerate(segments):
        start_time_sec = idx * step
        time_marker = seconds_to_hms(start_time_sec)
        print(f"Transcribing segment {idx} (start: {time_marker})...")
        text = transcribe_segment(seg_file)
        final_transcription += f"--- Segment {idx} (start: {time_marker}) ---\n{text}\n\n"
    
    # Step 4: Save final transcription to a text file in TEMP_DIR
    output_txt = os.path.join(TEMP_DIR, "transcription.txt")
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write(final_transcription)
    print(f"Final transcription saved to {output_txt}")
    
#    # Optional: Clean up temporary files
#    os.remove(audio_mp4)
#    os.remove(audio_wav)
#    for seg in segments:
#        os.remove(seg)
#
if __name__ == "__main__":
    main()


