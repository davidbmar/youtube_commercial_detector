#!/usr/bin/python3
import os
import re
import subprocess
import shutil
import argparse
import json
import boto3
import sys
import time
import uuid
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define constants
DEFAULT_TEMP_DIR = "./temp"
DEFAULT_BATCH_SIZE = 5  # Process 5 videos at a time
DEFAULT_S3_BUCKET = "2025-03-15-youtube-transcripts"
DEFAULT_S3_PREFIX = "transcripts"
DEFAULT_RESULTS_PREFIX = "results"

class YouTubePhraseScanner:
    def __init__(self, 
                 phrase="hustle", 
                 temp_dir=DEFAULT_TEMP_DIR, 
                 queue_url=None, 
                 region="us-east-1",
                 s3_bucket=DEFAULT_S3_BUCKET,
                 s3_prefix=DEFAULT_S3_PREFIX,
                 batch_size=DEFAULT_BATCH_SIZE,
                 use_gpu=True):
        self.phrase = phrase
        self.temp_dir = temp_dir
        self.queue_url = queue_url
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.batch_size = batch_size
        self.use_gpu = use_gpu
        
        # Initialize AWS clients
        self.s3 = boto3.client('s3', region_name=region)
        self.sqs = boto3.client('sqs', region_name=region) if queue_url else None
    
        # Check the bucket exists for writing.
        self.ensure_bucket_exists()

        # Create temp directory
        os.makedirs(temp_dir, exist_ok=True)
        
        # Import and initialize WhisperX if available
        self.whisperx_model = None
        if use_gpu:
            try:
                # Reduce TensorFlow logging
                os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
                
                import whisperx
                device = "cuda" if use_gpu else "cpu"
                compute_type = "float16" if use_gpu else "float32"
                
                logger.info(f"Loading WhisperX model (this may take a moment)...")
                self.whisperx_model = whisperx.load_model(
                    "large", device=device, compute_type=compute_type
                )
                self.device = device
                logger.info(f"WhisperX model loaded successfully")
            except ImportError:
                logger.error("WhisperX is not installed. Install via 'pip install whisperx'")
                raise
            except Exception as e:
                logger.error(f"Error loading WhisperX model: {str(e)}")
                raise

    def process_batch(self):
        """Process a batch of videos from the SQS queue"""
        processed_count = 0
        batch_results = []
        
        while processed_count < self.batch_size:
            youtube_url, custom_phrase = self._get_video_from_queue()
            if not youtube_url:
                logger.info("No more videos in queue or error occurred")
                break
                
            # Use custom phrase if provided in message, otherwise use default
            current_phrase = custom_phrase if custom_phrase else self.phrase
            
            # Create a video-specific temp directory
            video_id = self._extract_video_id(youtube_url)
            video_temp_dir = os.path.join(self.temp_dir, video_id)
            os.makedirs(video_temp_dir, exist_ok=True)
            
            try:
                # Then before processing:
                if self.results_exist_in_s3(video_id):
                     logging.info(f"Results for video {video_id} already exist in S3, skipping")
                     continue  # Skip to next video in queue 

                logger.info(f"Processing video {video_id} with phrase '{current_phrase}'")
                result = self._process_single_video(youtube_url, current_phrase, video_temp_dir)
                
                # Add video metadata
                result["video_id"] = video_id
                result["youtube_url"] = youtube_url
                result["phrase"] = current_phrase
                result["processed_at"] = datetime.now().isoformat()
                
                # Save results to S3
                self._save_results_to_s3(result, video_id)
                
                batch_results.append(result)
                processed_count += 1
                
                # Clean up video temp directory to save space
                #shutil.rmtree(video_temp_dir)
                
            except Exception as e:
                logger.error(f"Error processing video {video_id}: {str(e)}")
                # Continue with next video
        
        return batch_results

    def ensure_bucket_exists(self):
        """Check if the S3 bucket exists and create it if it doesn't"""
        try:
            self.s3.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"S3 bucket '{self.s3_bucket}' already exists")
        except self.s3.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                try:
                    # Create the bucket
                    if self.region == 'us-east-1':
                        self.s3.create_bucket(Bucket=self.s3_bucket)
                    else:
                        self.s3.create_bucket(
                            Bucket=self.s3_bucket,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    logger.info(f"Created S3 bucket '{self.s3_bucket}'")
                except Exception as create_error:
                    logger.error(f"Error creating S3 bucket: {str(create_error)}")
                    raise
            else:
                logger.error(f"Error checking S3 bucket: {str(e)}")
                raise

    #Check if this has already been processed, no sense in wasting compute money.
    def results_exist_in_s3(self, video_id):
        """Check if results for this video already exist in S3."""
        try:
            # Check if any results files exist for this video ID
            prefix = f"{DEFAULT_RESULTS_PREFIX}/{video_id}/"
            response = self.s3.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )
            return 'Contents' in response and len(response['Contents']) > 0
        except Exception as e:
            logging.error(f"Error checking if results exist: {e}")
            return False

    def _process_single_video(self, youtube_url, phrase, video_temp_dir):
        """Process a single YouTube video"""
        # Step 1: Download audio and convert to WAV
        audio_mp4 = self._download_audio(youtube_url, video_temp_dir)
        audio_wav = self._convert_to_wav(audio_mp4, video_temp_dir)

        # Step 2: Split audio into 60-second segments
        segments = self._split_audio(audio_wav, video_temp_dir)

        # Step 3: Transcribe each segment
        for seg_file in segments:
            transcription = self._transcribe_segment(seg_file)
            txt_file = seg_file.replace(".wav", ".txt")
            with open(txt_file, "w", encoding="utf-8") as f:
                f.write(transcription)
            
            # Upload transcription segment to S3
            self._upload_transcription_to_s3(txt_file, youtube_url)
        
        # Step 4: Scan transcripts for the phrase
        stats = self._scan_transcripts(video_temp_dir, phrase)
        
        # Return statistics
        return stats
    
    def _download_audio(self, youtube_url, temp_dir):
        """Download audio-only stream from YouTube"""
        logger.info(f"Downloading audio from {youtube_url}")
        from pytubefix import YouTube
        yt = YouTube(youtube_url)
        audio_stream = yt.streams.filter(only_audio=True).first()
        audio_file = os.path.join(temp_dir, "audio.mp4")
        audio_stream.download(output_path=temp_dir, filename="audio.mp4")
        return audio_file
    
    def _convert_to_wav(self, input_file, temp_dir):
        """Convert the MP4 audio file to WAV format"""
        output_file = os.path.join(temp_dir, "audio.wav")
        
        # Run ffmpeg with less output
        with open(os.devnull, 'w') as devnull:
            subprocess.run(["ffmpeg", "-y", "-i", input_file, output_file], 
                           check=True, stdout=devnull, stderr=devnull)
                           
        return output_file
    
    def _get_audio_duration(self, wav_file):
        """Get duration (in seconds) of the WAV file"""
        cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", wav_file
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        duration = float(result.stdout.strip())
        return duration
    
    def _split_audio(self, wav_file, temp_dir, segment_duration=60, overlap=0):
        """Split the WAV file into segments"""
        total_duration = self._get_audio_duration(wav_file)
        segments = []
        idx = 0
        start_time = 0
        
        while start_time < total_duration:
            output_file = os.path.join(temp_dir, f"segment_{idx:03d}.wav")
            
            # Run ffmpeg with less output
            with open(os.devnull, 'w') as devnull:
                subprocess.run([
                    "ffmpeg", "-y",
                    "-ss", str(start_time),
                    "-t", str(segment_duration),
                    "-i", wav_file,
                    output_file
                ], check=True, stdout=devnull, stderr=devnull)
                
            segments.append(output_file)
            
            # Log less frequently
            if idx % 5 == 0:
                logger.info(f"Created segment {idx} from {start_time:.0f} to {start_time + segment_duration:.0f} sec")
                
            idx += 1
            start_time += segment_duration
            
        logger.info(f"Created {len(segments)} segments total")
        return segments
    
    def _transcribe_segment(self, segment_file):
        """Transcribe an audio segment using WhisperX"""
        import whisperx
        
        segment_name = os.path.basename(segment_file)
        segment_num = int(segment_name.split('_')[1].split('.')[0])
        
        # Log less frequently
        if segment_num % 5 == 0:
            logger.info(f"Transcribing segment {segment_num}...")
        
        # Redirect stdout/stderr during WhisperX operations
        old_stdout, old_stderr = os.dup(1), os.dup(2)
        with open(os.devnull, 'w') as devnull:
            os.dup2(devnull.fileno(), 1)
            os.dup2(devnull.fileno(), 2)
            
            try:
                result = self.whisperx_model.transcribe(segment_file)
                language = result.get("language", "en")
                align_model, metadata = whisperx.load_align_model(language, self.device)
                result_aligned = whisperx.align(result["segments"], align_model, metadata, segment_file, self.device)
                
                transcription = ""
                for segment in result_aligned["segments"]:
                    transcription += segment["text"].strip() + " "
                    
                return transcription.strip()
            finally:
                # Restore stdout/stderr
                os.dup2(old_stdout, 1)
                os.dup2(old_stderr, 2)
                os.close(old_stdout)
                os.close(old_stderr)
    
    def _scan_transcripts(self, temp_dir, phrase):
        """Scan transcript files for the given phrase"""
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
                pattern = re.escape(phrase)
                occurrences = re.findall(pattern, content, re.IGNORECASE)
                count_occ = len(occurrences)
                total_occurrences += count_occ
                words = content.split()
                total_words += len(words)
                total_chars += len(content)
                if count_occ > 0:
                    minute_idx = int(txt_file.split('_')[1].split('.')[0])
                    files_with_phrase.append({
                        "filename": txt_file,
                        "minute": minute_idx + 1,
                        "occurrences": count_occ
                    })
        
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

    def _get_video_from_queue(self):
        # The queue_url isn't defined here, but it should be
        # It should either be:
        # 1. An instance variable set during initialization
        queue_url = self.queue_url  # If it's stored as an instance variable
        
        # 2. Or a parameter passed to this method
        # def _get_video_from_queue(self, queue_url):
        
        if not queue_url:
            logging.warning("No queue URL provided, exiting")
            sys.exit(0)  # Exit with success code
    
        try:
            # Get queue depth for monitoring
            queue_attributes = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            queue_depth = queue_attributes['Attributes'].get('ApproximateNumberOfMessages', '0')
            logger.info(f"Current queue depth: {queue_depth} messages")
            
            # If queue is empty, return None
            if queue_depth == '0':
                logger.info("Queue is empty")
                return None, None
                
            # Receive a message from the queue
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=5,
                VisibilityTimeout=600  # 10 minutes
            )
            
            # Check if a message was received
            if 'Messages' not in response or not response['Messages']:
                logger.info("No messages available in the queue")
                return None, None
                
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            message_id = message.get('MessageId', 'unknown')
            
            try:
                # Parse the message body
                body = json.loads(message['Body'])
                
                # Delete the message from the queue
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
                
                # Return the YouTube URL and custom phrase if provided
                if 'youtube_url' in body:
                    logger.info(f"Processing message: {message_id}")
                    phrase = body.get('phrase', None)
                    return body['youtube_url'], phrase
                else:
                    logger.error("Message does not contain a YouTube URL")
                    return None, None
            except json.JSONDecodeError:
                logger.error("Could not parse message body as JSON")
                return None, None
        except Exception as e:
            logger.error(f"Error receiving message from SQS: {str(e)}")
            return None, None
    
    def _extract_video_id(self, youtube_url):
        """Extract the video ID from a YouTube URL"""
        # Simple regex to extract video ID
        match = re.search(r'(?:v=|\/)([0-9A-Za-z_-]{11}).*', youtube_url)
        if match:
            return match.group(1)
        return str(uuid.uuid4())[:11]  # Fallback to UUID if unable to extract
    
    def _upload_transcription_to_s3(self, txt_file, youtube_url):
        """Upload a transcription file to S3"""
        video_id = self._extract_video_id(youtube_url)
        segment_name = os.path.basename(txt_file)
        s3_key = f"{self.s3_prefix}/{video_id}/{segment_name}"
        
        try:
            self.s3.upload_file(txt_file, self.s3_bucket, s3_key)
            # Log less frequently
            if '000' in segment_name or '005' in segment_name:
                logger.info(f"Uploaded {segment_name} to S3")
            return True
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            return False
    
    def _save_results_to_s3(self, results, video_id):
        """Save analysis results to S3"""
        # Create a unique results file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        s3_key = f"{DEFAULT_RESULTS_PREFIX}/{video_id}/{timestamp}-results.json"
        
        try:
            # Convert results to JSON
            results_json = json.dumps(results, indent=2)
            
            # Upload to S3
            self.s3.put_object(
                Body=results_json,
                Bucket=self.s3_bucket,
                Key=s3_key,
                ContentType='application/json'
            )
            
            logger.info(f"Results saved to s3://{self.s3_bucket}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error saving results to S3: {str(e)}")
            return False

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Scan YouTube videos for a given phrase and report statistics."
    )
    parser.add_argument(
        "--phrase", "-p",
        type=str,
        default="hustle",
        help="The phrase to search for (Default: 'hustle')"
    )
    parser.add_argument(
        "--temp_dir", "-t",
        type=str,
        default=DEFAULT_TEMP_DIR,
        help=f"Path to the temporary directory (Default: '{DEFAULT_TEMP_DIR}')"
    )
    parser.add_argument(
        "--queue_url", "-q",
        type=str,
        help="URL of the SQS queue to pull YouTube URLs from"
    )
    parser.add_argument(
        "--region", "-r",
        type=str,
        default="us-east-1",
        help="AWS region for AWS services. (Default: 'us-east-1')"
    )
    parser.add_argument(
        "--s3_bucket", "-b",
        type=str,
        default=DEFAULT_S3_BUCKET,
        help=f"S3 bucket to store transcriptions and results. (Default: '{DEFAULT_S3_BUCKET}')"
    )
    parser.add_argument(
        "--s3_prefix", "-s",
        type=str,
        default=DEFAULT_S3_PREFIX,
        help=f"S3 prefix for transcriptions. (Default: '{DEFAULT_S3_PREFIX}')"
    )
    parser.add_argument(
        "--batch_size", "-n",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of videos to process in one batch. (Default: {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--cpu", 
        action="store_true",
        help="Use CPU instead of GPU for transcription."
    )
    return parser.parse_args()

def main():
    args = parse_arguments()

    # At the beginning of your main function:
    if os.path.exists(args.temp_dir):
        shutil.rmtree(args.temp_dir)
    os.makedirs(args.temp_dir, exist_ok=True)
        
    # Initialize scanner
    scanner = YouTubePhraseScanner(
        phrase=args.phrase,
        temp_dir=args.temp_dir,
        queue_url=args.queue_url,
        region=args.region,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        batch_size=args.batch_size,
        use_gpu=not args.cpu
    )
    
    # Process a batch of videos
    logger.info(f"Starting batch processing with batch size {args.batch_size}")
    batch_results = scanner.process_batch()
    
    # Print summary of batch results
    logger.info("\n--- Batch Processing Summary ---")
    logger.info(f"Processed {len(batch_results)} videos")
    
    for idx, result in enumerate(batch_results):
        logger.info(f"\nVideo {idx+1}: {result['video_id']} ({result['youtube_url']})")
        logger.info(f"Phrase: '{result['phrase']}'")
        logger.info(f"Duration: {result['video_duration_min']:.2f} minutes")
        logger.info(f"Total occurrences: {result['total_occurrences']}")
        if result['files_with_phrase']:
            logger.info("Occurrences by minute:")
            for occurrence in result['files_with_phrase']:
                logger.info(f"  - Minute {occurrence['minute']}: {occurrence['occurrences']} occurrence(s)")
        else:
            logger.info("No occurrences found")

if __name__ == "__main__":
    main()
