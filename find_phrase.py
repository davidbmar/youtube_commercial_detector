#!/usr/bin/python3
import re

def find_phrase_in_transcription(transcription_file, phrase):
    """
    Reads the transcription file and searches for the given phrase in a case-insensitive manner.
    Each segment is assumed to start with a header line like:
      --- Segment {idx} (start: HH:MM:SS) ---
    If the phrase is found within a segment, the segment's start time is printed.
    """
    # Regex to capture segments: 
    # Capture the timestamp from a header and the following block of text until the next header or end of file.
    segment_pattern = r"--- Segment \d+ \(start: ([0-9:]+)\) ---\n(.*?)(?=\n--- Segment|\Z)"
    
    with open(transcription_file, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Find all segments (timestamp and corresponding text)
    segments = re.findall(segment_pattern, content, flags=re.DOTALL)
    
    phrase_lower = phrase.lower()
    found = False
    
    for timestamp, segment_text in segments:
        if phrase_lower in segment_text.lower():
            print(f"Phrase '{phrase}' found in segment starting at {timestamp}")
            found = True
    
    if not found:
        print(f"Phrase '{phrase}' not found in transcription.")

def main():
    transcription_file = "./temp/transcription.txt"  # Update if needed
    phrase = "so a lot of people"
    find_phrase_in_transcription(transcription_file, phrase)

if __name__ == "__main__":
    main()

