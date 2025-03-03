#!/usr/bin/python3
import os
import re
import argparse
import shutil

DEFAULT_TEMP_DIR = "./temp"

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Search transcript files in a directory for a given phrase."
    )
    parser.add_argument(
        "--phrase", "-p",
        type=str,
        required=True,
        help="The phrase to search for (can be multiple words, case-insensitive)."
    )
    parser.add_argument(
        "--temp_dir", "-t",
        type=str,
        default=DEFAULT_TEMP_DIR,
        help=f"Path to the directory containing transcript (.txt) files (default: '{DEFAULT_TEMP_DIR}')."
    )
    return parser.parse_args()

def clear_temp_dir(temp_dir):
    """(Optional) Delete and recreate the temporary directory."""
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

def seconds_to_hms(seconds):
    """Convert seconds to HH:MM:SS format."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

def scan_transcripts(temp_dir, phrase):
    """
    Scan all transcript (.txt) files in temp_dir for the given phrase.
    Returns overall statistics:
      - video_duration_sec (assuming each segment is 60 sec)
      - total_words, total_chars across all files
      - total_occurrences of the phrase
      - list of files (by minute) where the phrase was found along with occurrence counts.
    """
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
            # Escape the phrase in case it contains special regex characters.
            pattern = re.escape(phrase)
            occurrences = re.findall(pattern, content, re.IGNORECASE)
            count_occ = len(occurrences)
            total_occurrences += count_occ
            words = content.split()
            total_words += len(words)
            total_chars += len(content)
            if count_occ > 0:
                # Assume file naming convention "segment_XXX.txt", extract segment number.
                minute_idx = int(txt_file.split('_')[1].split('.')[0])
                files_with_phrase.append((txt_file, minute_idx, count_occ))
    
    num_segments = len(txt_files)
    video_duration_sec = num_segments * 60  # Each segment is 60 sec
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

    # Optional: Uncomment to clear the temp directory.
    # clear_temp_dir(temp_dir)

    stats = scan_transcripts(temp_dir, phrase)
    
    print("\n--- Scan Results ---")
    print(f"Video duration: {stats['video_duration_sec']} seconds ({stats['video_duration_min']:.2f} minutes)")
    print(f"Total words scanned: {stats['total_words']}")
    print(f"Total characters scanned: {stats['total_chars']}")
    print(f"Total occurrences of '{phrase}': {stats['total_occurrences']}\n")
    if stats["files_with_phrase"]:
        print("Files (by minute) containing the phrase:")
        for filename, minute_idx, occ in stats["files_with_phrase"]:
            print(f"  - {filename} (Minute {minute_idx + 1}): {occ} occurrence(s)")
    else:
        print(f"No occurrences of '{phrase}' found in any transcript files.")

if __name__ == "__main__":
    main()

