#!/usr/bin/python3
import os
import subprocess
import requests
import time
from pytubefix import YouTube

def download_audio(youtube_url):
    """Download audio-only stream from YouTube using pytubefix."""
    yt = YouTube(youtube_url)
    audio_stream = yt.streams.filter(only_audio=True).first()
    audio_file = audio_stream.download(output_path=".", filename="audio.mp4")
    print(f"Downloaded audio file: {audio_file}")
    return audio_file

def convert_to_wav(input_file):
    """Convert the MP4 audio file to WAV format using ffmpeg."""
    output_file = "audio.wav"
    subprocess.run(["ffmpeg", "-y", "-i", input_file, output_file], check=True)
    print(f"Converted audio to WAV: {output_file}")
    return output_file

def split_audio(input_wav, segment_time=600):
    """
    Split the WAV file into segments of a specified duration (in seconds).
    For 10 minutes, use segment_time=600.
    The segments will be named segment_000.wav, segment_001.wav, etc.
    """
    output_pattern = "segment_%03d.wav"
    cmd = [
        "ffmpeg", "-y", "-i", input_wav,
        "-f", "segment",
        "-segment_time", str(segment_time),
        "-c", "copy",
        output_pattern
    ]
    subprocess.run(cmd, check=True)
    segments = sorted([f for f in os.listdir(".") if f.startswith("segment_") and f.endswith(".wav")])
    print(f"Created {len(segments)} segments.")
    return segments

def transcribe_segment(segment_file, retries=3, backoff_factor=2):
    """Transcribe an audio segment via the local API with retries."""
    api_url = "http://localhost:8000/v1/audio/transcriptions"  # update if needed
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
    """Convert seconds to a HH:MM:SS string."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

def main():
    youtube_url = "https://www.youtube.com/watch?v=TOQtJch3kGk"

    # Step 1: Download audio and convert to WAV
    audio_mp4 = download_audio(youtube_url)
    audio_wav = convert_to_wav(audio_mp4)
    
    # Step 2: Split audio into 10-minute segments (600 seconds)
    segments = split_audio(audio_wav, segment_time=600)
    
    # Step 3: Transcribe each segment and accumulate results with time markers
    final_transcription = ""
    for idx, seg_file in enumerate(segments):
        start_time_sec = idx * 600  # each segment starts at idx * 600 seconds
        time_marker = seconds_to_hms(start_time_sec)
        print(f"Transcribing segment {idx} (start: {time_marker})...")
        text = transcribe_segment(seg_file)
        final_transcription += f"--- Segment {idx} (start: {time_marker}) ---\n{text}\n\n"
    
    # Step 4: Save final transcription to a text file
    output_txt = "transcription.txt"
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write(final_transcription)
    print(f"Final transcription saved to {output_txt}")
    
#    # Optional Cleanup: Remove temporary files
#    os.remove(audio_mp4)
#    os.remove(audio_wav)
#    for seg in segments:
#        os.remove(seg)
#
if __name__ == "__main__":
    main()

