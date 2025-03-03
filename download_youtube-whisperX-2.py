#!/usr/bin/python3
import os
import re
import subprocess
import shutil
import argparse

# Define the temporary directory
DEFAULT_TEMP_DIR = "./temp"

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Scan transcript files for a given phrase and report statistics."
    )
    parser.add_argument(
        "--phrase", "-p",
        type=str,
        default="hustle",
        help="The phrase to search for (can be multiple words). (Default: 'hustle')"
    )
    parser.add_argument(
        "--temp_dir", "-t",
        type=str,
        default=DEFAULT_TEMP_DIR,
        help=f"Path to the temporary directory containing transcript files (Default: '{DEFAULT_TEMP_DIR}')."
    )
    return parser.parse_args()

def clear_temp_dir(temp_dir):
    """Delete and recreate the temporary directory."""
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

def download_audio(youtube_url, temp_dir):
    """Download audio-only stream from YouTube using pytubefix into temp_dir."""
    from pytubefix import YouTube
    yt = YouTube(youtube_url)
    audio_stream = yt.streams.filter(only_audio=True).first()
    audio_file = os.path.join(temp_dir, "audio.mp4")
    audio_stream.download(output_path=temp_dir, filename="audio.mp4")
    print(f"Downloaded audio file: {audio_file}")
    return audio_file

def convert_to_wav(input_file, temp_dir):
    """Convert the MP4 audio file to WAV format using ffmpeg in temp_dir."""
    output_file = os.path.join(temp_dir, "audio.wav")
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

def split_audio_sliding(wav_file, temp_dir, segment_duration=60, overlap=0):
    """
    Split the WAV file into non-overlapping segments.
    With segment_duration=60 and overlap=0:
      - Segment 0:  0 to 60 sec, Segment 1: 60 to 120 sec, etc.
    Each segment is saved in temp_dir with names segment_XXX.wav.
    Returns a list of segment file paths.
    """
    total_duration = get_audio_duration(wav_file)
    segments = []
    idx = 0
    start_time = 0
    while start_time < total_duration:
        output_file = os.path.join(temp_dir, f"segment_{idx:03d}.wav")
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
        start_time += segment_duration
    print(f"Created {len(segments)} segments.")
    return segments

def transcribe_segment_whisperx(segment_file, model, device="cpu"):
    """
    Transcribe an audio segment using WhisperX locally.
    Returns the full transcription text with word-level timestamps.
    """
    import whisperx

    print(f"WhisperX transcribing {segment_file} ...")
    result = model.transcribe(segment_file)
    language = result.get("language", "en")
    align_model, metadata = whisperx.load_align_model(language, device)
    result_aligned = whisperx.align(result["segments"], align_model, metadata, segment_file, device)
    
    transcription = ""
    for segment in result_aligned["segments"]:
        transcription += segment["text"].strip() + " "
    return transcription.strip()

def scan_transcripts(temp_dir, phrase):
    """Scan all transcript (.txt) files in temp_dir for the given phrase and compute statistics."""
    txt_files = [f for f in os.listdir(temp_dir) if f.startswith("segment_") and f.endswith(".txt")]
    txt_files = sorted(txt_files)
    
    total_occurrences = 0
    total_words = 0
    total_chars = 0
    files_with_phrase = []
    
    for txt_file in txt_files:
        file_path = os.path.join(temp_dir, txt_file)
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            # Use regex to count occurrences of the phrase (case-insensitive)
            pattern = re.escape(phrase)
            occurrences = re.findall(pattern, content, re.IGNORECASE)
            count_occ = len(occurrences)
            total_occurrences += count_occ
            words = content.split()
            total_words += len(words)
            total_chars += len(content)
            if count_occ > 0:
                # Extract minute index from filename (e.g., segment_007.txt -> minute 7)
                minute_idx = int(txt_file.split('_')[1].split('.')[0])
                files_with_phrase.append((txt_file, minute_idx, count_occ))
                
    num_segments = len(txt_files)
    video_duration_sec = num_segments * 60
    
    stats = {
        "video_duration_sec": video_duration_sec,
        "video_duration_min": video_duration_sec / 60,
        "total_words": total_words,
        "total_chars": total_chars,
        "total_occurrences": total_occurrences,
        "files_with_phrase": files_with_phrase
    }
    return stats

def main():
    args = parse_arguments()
    phrase = args.phrase
    temp_dir = args.temp_dir

    # Clear out the temp directory
    clear_temp_dir(temp_dir)
    
    youtube_url = "https://www.youtube.com/watch?v=TOQtJch3kGk"
    
    # Step 1: Download audio and convert to WAV.
    audio_mp4 = download_audio(youtube_url, temp_dir)
    audio_wav = convert_to_wav(audio_mp4, temp_dir)

    # Step 2: Split audio into 60-second segments.
    segments = split_audio_sliding(audio_wav, temp_dir, segment_duration=60, overlap=0)

    # Step 3: Load the WhisperX model (using GPU on a 3070).
    use_whisperx_local = True
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

    # Step 4: Transcribe each segment and save transcription in corresponding .txt file.
    for seg_file in segments:
        transcription = transcribe_segment_whisperx(seg_file, model, device=device)
        txt_file = seg_file.replace(".wav", ".txt")
        with open(txt_file, "w", encoding="utf-8") as f:
            f.write(transcription)
        print(f"Transcription for {seg_file} saved to {txt_file}")
    
    # Step 5: Scan all transcript files for the given phrase.
    stats = scan_transcripts(temp_dir, phrase)
    
    print("\n--- Scan Results ---")
    print(f"Video duration: {stats['video_duration_sec']} sec ({stats['video_duration_min']:.2f} minutes)")
    print(f"Total words scanned: {stats['total_words']}")
    print(f"Total characters scanned: {stats['total_chars']}")
    print(f"Total occurrences of '{phrase}': {stats['total_occurrences']}")
    print("Files (by minute) containing the phrase:")
    for filename, minute_idx, occ in stats["files_with_phrase"]:
        print(f"  - {filename} (Minute {minute_idx + 1}): {occ} occurrence(s)")

if __name__ == "__main__":
    main()

