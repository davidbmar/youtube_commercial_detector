#!/usr/bin/python3
import os
import subprocess
import shutil
from pytubefix import YouTube

# Define the temporary directory
TEMP_DIR = "./temp"

# Before processing, delete the TEMP_DIR if it exists and recreate it.
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
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

def split_audio_sliding(wav_file, segment_duration=60, overlap=0):
    """
    Split the WAV file into non-overlapping segments.
    With segment_duration=60 and overlap=0:
      - Segment 0:  0 to 60 sec, Segment 1: 60 to 120 sec, etc.
    Each segment is saved in TEMP_DIR with names segment_000.wav, segment_001.wav, etc.
    Returns a list of segment file paths.
    """
    total_duration = get_audio_duration(wav_file)
    segments = []
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
        start_time += segment_duration  # non-overlapping segments
    print(f"Created {len(segments)} segments.")
    return segments

def transcribe_segment_whisperx(segment_file, model, device="cpu"):
    """
    Transcribe an audio segment using WhisperX locally.
    Returns the full transcription text along with word-level timestamps.
    """
    import whisperx

    print(f"WhisperX transcribing {segment_file} ...")
    # Removed beam_size parameter since it's unsupported.
    result = model.transcribe(segment_file)
    language = result.get("language", "en")
    align_model, metadata = whisperx.load_align_model(language, device)
    result_aligned = whisperx.align(result["segments"], align_model, metadata, segment_file, device)
    
    transcription = ""
    word_timestamps = []
    for segment in result_aligned["segments"]:
        transcription += segment["text"].strip() + " "
        if "words" in segment:
            word_timestamps.extend(segment["words"])
    transcription = transcription.strip()
    
    return transcription + "\nWord Timestamps: " + str(word_timestamps)

def main():
    youtube_url = "https://www.youtube.com/watch?v=TOQtJch3kGk"
    
    # Use WhisperX locally.
    use_whisperx_local = True

    # Step 1: Download audio and convert to WAV.
    audio_mp4 = download_audio(youtube_url)
    audio_wav = convert_to_wav(audio_mp4)

    # Step 2: Split the audio into 60-second segments (non-overlapping).
    segments = split_audio_sliding(audio_wav, segment_duration=60, overlap=0)

    # Step 3: Load the WhisperX model (using GPU on a 3070).
    if use_whisperx_local:
        try:
            import whisperx
        except ImportError:
            print("WhisperX is not installed. Please install it via 'pip install whisperx'")
            return
        model = whisperx.load_model("large", device="cuda", compute_type="float16")
        device = "cuda"
    else:
        model = None

    # Step 4: For each segment, transcribe and write the transcription to a corresponding .txt file.
    for seg_file in segments:
        transcription = transcribe_segment_whisperx(seg_file, model, device=device)
        txt_file = seg_file.replace(".wav", ".txt")
        with open(txt_file, "w", encoding="utf-8") as f:
            f.write(transcription)
        print(f"Transcription for {seg_file} saved to {txt_file}")

if __name__ == "__main__":
    main()

