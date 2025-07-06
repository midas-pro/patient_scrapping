import requests
import csv
import json
from typing import List, Dict, Optional
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime, timedelta
import random
import os
import getpass
import signal
import sys
import atexit

class RateLimiter:
    """Thread-safe rate limiter with randomized delays to appear more human-like"""
    def __init__(self, max_requests_per_second: float = 1.0, randomize_delay: bool = True):
        self.max_requests_per_second = max_requests_per_second
        self.base_interval = 1.0 / max_requests_per_second
        self.randomize_delay = randomize_delay
        self.last_request_time = 0
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            # Calculate required wait time
            required_interval = self.base_interval
            
            # Add randomization to appear more human-like (±30% variation)
            if self.randomize_delay:
                variation = random.uniform(0.7, 1.3)
                required_interval *= variation
            
            if time_since_last < required_interval:
                sleep_time = required_interval - time_since_last
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()

class PatientDataExtractor:
    def __init__(self, bearer_token: str, requests_per_second: float = 1.0, max_retries: int = 3, 
                 batch_size: int = 50, use_human_like_delays: bool = True):
        self.bearer_token = bearer_token
        self.headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'  # More human-like
        }
        self.base_url = "https://api-v2.smile2impress.com/gws/treatment-admin"
        self.rate_limiter = RateLimiter(requests_per_second, use_human_like_delays)
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.customer_ids_cache_file = "customer_ids_cache.json"
        
        # Track collected data for emergency CSV creation
        self.collected_data = []
        self.output_filename = 'patient_data.csv'
        
        # Setup signal handlers for graceful shutdown
        self.setup_signal_handlers()
        
        # Register cleanup function to run on exit
        atexit.register(self.emergency_csv_creation)
        
    def setup_signal_handlers(self):
        """Setup signal handlers to catch interruptions and create CSV before exit"""
        def signal_handler(signum, frame):
            print(f"\n🚨 Received signal {signum}. Creating emergency CSV...")
            self.emergency_csv_creation()
            print("🔄 Signal handler complete. Exiting gracefully...")
            sys.exit(0)
        
        # Handle common termination signals
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)  # Termination signal
        if hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, signal_handler)  # Windows Ctrl+Break
    
    def emergency_csv_creation(self):
        """Create CSV from any collected data - called on emergency exit"""
        try:
            # Try to load any existing checkpoint data
            checkpoint_data = self.load_all_available_data()
            
            if checkpoint_data:
                emergency_filename = f"emergency_patient_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                self.write_to_csv(checkpoint_data, emergency_filename)
                print(f"🚨 EMERGENCY CSV CREATED: {emergency_filename}")
                print(f"📊 Saved {len(checkpoint_data)} patient records")
            elif self.collected_data:
                emergency_filename = f"emergency_patient_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                self.write_to_csv(self.collected_data, emergency_filename)
                print(f"🚨 EMERGENCY CSV CREATED: {emergency_filename}")
                print(f"📊 Saved {len(self.collected_data)} patient records")
        except Exception as e:
            print(f"❌ Error creating emergency CSV: {e}")
    
    def load_all_available_data(self) -> List[Dict]:
        """Load all available data from checkpoints and memory"""
        all_data = []
        
        # Add in-memory collected data
        if self.collected_data:
            all_data.extend(self.collected_data)
            print(f"📊 Found {len(self.collected_data)} records in memory")
        
        # Try to load from checkpoint
        try:
            checkpoint_file = "progress_checkpoint.json"
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                
                checkpoint_customers = checkpoint_data.get('processed_customers', [])
                
                # Merge data, avoiding duplicates based on customerId
                existing_ids = {item.get('customerId') for item in all_data if 'customerId' in item}
                
                for customer in checkpoint_customers:
                    if customer.get('customerId') not in existing_ids:
                        all_data.append(customer)
                
                print(f"📊 Loaded {len(checkpoint_customers)} records from checkpoint")
        except Exception as e:
            print(f"⚠️ Could not load checkpoint data: {e}")
        
        return all_data
        
    def get_patient_actions(self) -> List[Dict]:
        """
        Fetch all patient actions from the first API
        Returns a list of patient action records
        """
        url = f"{self.base_url}/v2/getPatientActions"
        payload ={
    "after": "",
    "limit": 6000,
    "types": [
        "NEW_SCANS",
        "ENGAGERS_BOOK_LATE",
        "ENGAGERS_BOOKED_EARLY",
        "IPR_BOOK_LATE",
        "IPR_BOOKED_EARLY",
        "CLINIC_CHECK_UP",
        "CLINIC_CHECK_UP_STEP_BOOK_LATE",
        "PATIENT_NEED_VISIT",
        "PATIENT_NEED_EXTRA_VISIT",
        "PATIENT_RECEIVED_ALIGNERS_KIT",
        "PATIENT_RECEIVED_REFINEMENT_KIT",
        "PATIENT_RECEIVED_RETAINERS_KIT",
        "MESSAGE",
        "END_OF_TREATMENT_SOON"
    ],
    "languages": [],
    "countries": [],
    "dwBadge": [],
    "organization": [],
    "order": "DESC",
    "isWithoutCountry": False,
    "isUnassigned": False,
    "badges": [],
    "patientCareIds": []
}
        print("Fetching patient actions...")
        response = self.make_request_with_retry('POST', url, headers=self.headers,json=payload)
        
        if response:
            data = response.json()
            print(data)
            print(f"Successfully fetched {len(data['data']['items'])} patient actions")
            return data['data']['items']
        else:
            print("Failed to fetch patient actions")
            return []
        
    def update_bearer_token(self, new_token: str):
        """Update the bearer token and headers"""
        self.bearer_token = new_token
        self.headers['Authorization'] = f'Bearer {new_token}'
        print("✅ Bearer token updated successfully!")

    def make_request_with_retry(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and rate limiting"""
        for attempt in range(self.max_retries):
            try:
                # Apply rate limiting
                self.rate_limiter.wait_if_needed()
                
                # Make the request
                if method.upper() == 'POST':
                    response = requests.post(url, **kwargs)
                else:
                    response = requests.get(url, **kwargs)
                
                # Check for authentication error
                if response.status_code == 401:
                    print("\n🚨 AUTHENTICATION ERROR (401 Unauthorized)")
                    print("Your bearer token appears to be invalid or expired.")
                    print("Please provide a new bearer token to continue.\n")
                    
                    try:
                        new_token = get_bearer_token(is_retry=True)
                        self.update_bearer_token(new_token)
                        
                        # Update the headers in the current request kwargs
                        if 'headers' in kwargs:
                            kwargs['headers']['Authorization'] = f'Bearer {new_token}'
                        else:
                            kwargs['headers'] = self.headers
                        
                        print("🔄 Retrying request with new token...")
                        # Retry the request immediately with the new token
                        if method.upper() == 'POST':
                            response = requests.post(url, **kwargs)
                        else:
                            response = requests.get(url, **kwargs)
                        
                        # Check if the new token works
                        if response.status_code == 401:
                            print("❌ New token is also invalid. Please check your credentials.")
                            return None
                        
                    except KeyboardInterrupt:
                        print("\n❌ Token update cancelled by user.")
                        return None
                    except Exception as e:
                        print(f"❌ Error updating token: {e}")
                        return None
                
                # Check for rate limiting response
                if response.status_code == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', 5))
                    print(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                # Check if it's a 401 error that wasn't caught above
                if hasattr(e, 'response') and e.response is not None and e.response.status_code == 401:
                    print("\n🚨 AUTHENTICATION ERROR (401 Unauthorized)")
                    print("Your bearer token appears to be invalid or expired.")
                    print("Please provide a new bearer token to continue.\n")
                    
                    try:
                        new_token = get_bearer_token(is_retry=True)
                        self.update_bearer_token(new_token)
                        
                        # Update the headers in the current request kwargs
                        if 'headers' in kwargs:
                            kwargs['headers']['Authorization'] = f'Bearer {new_token}'
                        else:
                            kwargs['headers'] = self.headers
                        
                        print("🔄 Retrying request with new token...")
                        continue  # Retry with new token
                        
                    except KeyboardInterrupt:
                        print("\n❌ Token update cancelled by user.")
                        return None
                    except Exception as token_error:
                        print(f"❌ Error updating token: {token_error}")
                        return None
                
                print(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    # Exponential backoff
                    wait_time = (2 ** attempt) + 1
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Max retries exceeded for {url}")
                    return None
    
    def save_customer_ids_to_cache(self, customer_ids: List[int]):
        """Save customer IDs to a cache file for recovery purposes"""
        cache_data = {
            'customer_ids': customer_ids,
            'timestamp': datetime.now().isoformat(),
            'total_count': len(customer_ids)
        }
        
        try:
            with open(self.customer_ids_cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            print(f"✅ Saved {len(customer_ids)} customer IDs to cache file: {self.customer_ids_cache_file}")
        except Exception as e:
            print(f"⚠️ Failed to save customer IDs to cache: {e}")
    
    def load_customer_ids_from_cache(self) -> Optional[List[int]]:
        """Load customer IDs from cache file if it exists"""
        try:
            if os.path.exists(self.customer_ids_cache_file):
                with open(self.customer_ids_cache_file, 'r') as f:
                    cache_data = json.load(f)
                
                customer_ids = cache_data.get('customer_ids', [])
                timestamp = cache_data.get('timestamp', '')
                
                print(f"📁 Found cached customer IDs from {timestamp}")
                print(f"📊 Cache contains {len(customer_ids)} customer IDs")
                
                # Ask user if they want to use cached data
                use_cache = input("Do you want to use cached customer IDs? (y/n): ").lower().strip()
                if use_cache in ['y', 'yes']:
                    return customer_ids
                else:
                    print("🔄 Will fetch fresh customer IDs...")
                    return None
            return None
        except Exception as e:
            print(f"⚠️ Error loading cache: {e}")
            return None
    
    def save_progress_checkpoint(self, processed_customers: List[Dict], filename: str = "progress_checkpoint.json"):
        """Save progress checkpoint to resume if interrupted AND create CSV backup"""
        checkpoint_data = {
            'processed_customers': processed_customers,
            'timestamp': datetime.now().isoformat(),
            'count': len(processed_customers)
        }
        
        try:
            # Save JSON checkpoint
            with open(filename, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            print(f"💾 Checkpoint saved: {len(processed_customers)} customers processed")
            
            # ALWAYS create/update CSV backup when checkpoint is saved
            if processed_customers:
                backup_csv_filename = f"backup_{self.output_filename}"
                self.write_to_csv(processed_customers, backup_csv_filename)
                print(f"📄 Backup CSV updated: {backup_csv_filename}")
                
        except Exception as e:
            print(f"⚠️ Failed to save checkpoint: {e}")
    
    def load_progress_checkpoint(self, filename: str = "progress_checkpoint.json") -> List[Dict]:
        """Load progress checkpoint if it exists"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    checkpoint_data = json.load(f)
                
                processed_customers = checkpoint_data.get('processed_customers', [])
                timestamp = checkpoint_data.get('timestamp', '')
                
                print(f"📁 Found progress checkpoint from {timestamp}")
                print(f"📊 Checkpoint contains {len(processed_customers)} processed customers")
                
                use_checkpoint = input("Do you want to resume from checkpoint? (y/n): ").lower().strip()
                if use_checkpoint in ['y', 'yes']:
                    return processed_customers
                else:
                    print("🔄 Starting fresh...")
                    return []
            return []
        except Exception as e:
            print(f"⚠️ Error loading checkpoint: {e}")
            return []
        
        return None
    
    def get_patient_details(self, patient_id: int) -> Optional[Dict]:
        """
        Fetch detailed patient information using patient ID
        """
        url = f"{self.base_url}/patients/v1/getPatient"
        payload = {"patientId": patient_id}
        
        response = self.make_request_with_retry('POST', url, headers=self.headers, json=payload)
        
        if response:
            return response.json()
        else:
            print(f"Failed to fetch patient details for ID {patient_id}")
            return None
    
    def extract_patient_info(self, patient_data: Dict) -> Dict:
        print(patient_data)
        """
        Extract required fields from patient data
        """
        return {
            'customerId': patient_data['data']['id'],
            'email':patient_data['data']['email'],
            'phone': patient_data['data']['phone'],
            'firstName': patient_data['data']['firstName'],
            'lastName': patient_data['data']['lastName']
        }
    
    def process_patient_batch(self, customer_ids: List[int]) -> List[Dict]:
        """Process a batch of customer IDs"""
        batch_results = []
        
        for customer_id in customer_ids:
            patient_data = self.get_patient_details(customer_id)
            if patient_data:
                extracted_info = self.extract_patient_info(patient_data)
                batch_results.append(extracted_info)
                # Add to in-memory collection for emergency scenarios
                self.collected_data.append(extracted_info)
        
        return batch_results

    def process_all_patients(self, output_filename: str = 'patient_data.csv', use_threading: bool = False):
        """
        Main function to process all patients and create CSV with intelligent caching and recovery
        """
        self.output_filename = output_filename  # Store for emergency use
        
        print("🚀 Starting Patient Data Extraction Process")
        print("=" * 60)
        
        try:
            # Step 0: Try to load existing progress
            processed_customers = self.load_progress_checkpoint()
            processed_customer_ids = {customer['customerId'] for customer in processed_customers}
            
            # Add processed customers to collected_data for emergency scenarios
            self.collected_data.extend(processed_customers)
            
            # Step 1: Get customer IDs (try cache first, then API)
            customer_ids = self.load_customer_ids_from_cache()
            
            if not customer_ids:
                print("\n📡 Fetching patient actions from API...")
                patient_actions = self.get_patient_actions()
                
                if not patient_actions:
                    print("❌ No patient actions found. Exiting.")
                    return
                
                # Extract unique customer IDs and save to cache
                customer_ids = list(set(
                    action['customerId'] for action in patient_actions 
                    if 'customerId' in action
                ))
                
                # Save customer IDs to cache for future use
                self.save_customer_ids_to_cache(customer_ids)
            
            print(f"\n📊 Total unique customer IDs: {len(customer_ids)}")
            
            # Filter out already processed customers
            remaining_customer_ids = [cid for cid in customer_ids if cid not in processed_customer_ids]
            
            if processed_customers:
                print(f"✅ Already processed: {len(processed_customers)} customers")
                print(f"⏳ Remaining to process: {len(remaining_customer_ids)} customers")
            
            if not remaining_customer_ids:
                print("🎉 All customers already processed!")
                if processed_customers:
                    self.write_to_csv(processed_customers, output_filename)
                    print(f"📄 CSV file updated: {output_filename}")
                return
            
            print(f"\n🔄 Processing mode: {'Multi-threaded' if use_threading else 'Sequential (Safer)'}")
            print(f"⚡ Rate limit: {self.rate_limiter.max_requests_per_second} requests/second")
            print(f"🔄 Max retries: {self.max_retries}")
            print("\n" + "=" * 60)
            
            # Step 2: Process remaining patients
            if use_threading and len(remaining_customer_ids) > 20:
                new_patient_details = self._process_with_threading(remaining_customer_ids, processed_customers)
            else:
                new_patient_details = self._process_sequentially(remaining_customer_ids, processed_customers)
            
            # Step 3: Combine all results
            all_patient_details = processed_customers + new_patient_details
            
            # Step 4: Write final CSV
            if all_patient_details:
                self.write_to_csv(all_patient_details, output_filename)
                print(f"\n🎉 SUCCESS! Exported {len(all_patient_details)} patient records to {output_filename}")
                
                # Clean up checkpoint file
                try:
                    if os.path.exists("progress_checkpoint.json"):
                        os.remove("progress_checkpoint.json")
                        print("🧹 Cleaned up checkpoint file")
                except:
                    pass
            else:
                print("❌ No patient details were successfully fetched.")
                
        except KeyboardInterrupt:
            print("\n🚨 Process interrupted by user (Ctrl+C)")
            self.emergency_csv_creation()
            raise
        except Exception as e:
            print(f"\n❌ Unexpected error occurred: {e}")
            self.emergency_csv_creation()
            raise
    
    def _process_sequentially(self, customer_ids: List[int], existing_customers: List[Dict] = None) -> List[Dict]:
        """Process customer IDs one by one with anti-DDoS measures"""
        if existing_customers is None:
            existing_customers = []
            
        patient_details = []
        total_customers = len(customer_ids)
        checkpoint_interval = 25  # Save progress every 25 customers
        
        print(f"\n🐌 Sequential Processing Started")
        print(f"📈 Will save checkpoint every {checkpoint_interval} customers")
        
        try:
            for i, customer_id in enumerate(customer_ids, 1):
                print(f"🔄 Processing customer {i}/{total_customers}: ID {customer_id}")
                
                # Add random delay every few requests to appear more human-like
                if i % 10 == 0:
                    extra_delay = random.uniform(2, 5)
                    print(f"😴 Taking a short break ({extra_delay:.1f}s) to be nice to the server...")
                    time.sleep(extra_delay)
                
                patient_data = self.get_patient_details(customer_id)
                
                if patient_data:
                    extracted_info = self.extract_patient_info(patient_data)
                    patient_details.append(extracted_info)
                    # Add to in-memory collection for emergency scenarios
                    self.collected_data.append(extracted_info)
                    print(f"   ✅ Success: {extracted_info.get('firstName', '')} {extracted_info.get('lastName', '')}")
                else:
                    print(f"   ❌ Failed to get details for ID {customer_id}")
                
                # Save checkpoint periodically
                if i % checkpoint_interval == 0:
                    all_processed = existing_customers + patient_details
                    self.save_progress_checkpoint(all_processed)
                    print(f"💾 Checkpoint saved at {i}/{total_customers}")
                
                # Progress update
                if i % 10 == 0:
                    success_rate = (len(patient_details) / i) * 100
                    print(f"📊 Progress: {i}/{total_customers} ({(i/total_customers)*100:.1f}%) | Success rate: {success_rate:.1f}%")
            
            # Final checkpoint
            all_processed = existing_customers + patient_details
            self.save_progress_checkpoint(all_processed)
            
        except Exception as e:
            print(f"❌ Error during sequential processing: {e}")
            # Create emergency CSV with current progress
            all_processed = existing_customers + patient_details
            if all_processed:
                self.save_progress_checkpoint(all_processed)
            raise
        
        return patient_details
    
    def _process_with_threading(self, customer_ids: List[int], existing_customers: List[Dict] = None) -> List[Dict]:
        """Process customer IDs using thread pool with enhanced safety measures"""
        if existing_customers is None:
            existing_customers = []
            
        print("🚀 Multi-threaded Processing Started (Conservative Mode)")
        patient_details = []
        
        try:
            # Split customer IDs into batches
            batches = [customer_ids[i:i + self.batch_size] for i in range(0, len(customer_ids), self.batch_size)]
            
            # Use only 2 threads to be extra conservative
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_batch = {
                    executor.submit(self.process_patient_batch, batch): batch 
                    for batch in batches
                }
                
                completed = 0
                for future in as_completed(future_to_batch):
                    batch_results = future.result()
                    patient_details.extend(batch_results)
                    completed += len(future_to_batch[future])
                    
                    # Save checkpoint after each batch
                    all_processed = existing_customers + patient_details
                    self.save_progress_checkpoint(all_processed)
                    
                    print(f"📊 Progress: {completed}/{len(customer_ids)} ({(completed/len(customer_ids))*100:.1f}%)")
                    
                    # Add delay between batches
                    if completed < len(customer_ids):
                        batch_delay = random.uniform(3, 7)
                        print(f"😴 Batch completed. Waiting {batch_delay:.1f}s before next batch...")
                        time.sleep(batch_delay)
        except Exception as e:
            print(f"❌ Error during threaded processing: {e}")
            # Create emergency CSV with current progress
            all_processed = existing_customers + patient_details
            if all_processed:
                self.save_progress_checkpoint(all_processed)
            raise
      
        return patient_details
    
    def write_to_csv(self, patient_data: List[Dict], filename: str):
        """
        Write patient data to CSV file
        """
        print("i am here")
        print(patient_data)
        if not patient_data:
            return
        
        fieldnames = ['customerId', 'email', 'phone', 'firstName', 'lastName', 'fullName', 'country', 'language']
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(patient_data)
            print(f"✅ CSV file created/updated: {filename}")
        except Exception as e:
            print(f"❌ Error writing CSV file {filename}: {e}")

def get_bearer_token(is_retry: bool = False) -> str:
    """
    Securely prompt for bearer token with fallback options
    """
    if is_retry:
        print("🔐 Bearer Token Re-Authentication")
        print("-" * 40)
        print("Please enter a new/updated bearer token.")
    else:
        print("🔐 Bearer Token Authentication")
        print("-" * 40)
        print("Please enter your bearer token for API authentication.")
    
    print("\nInput method options:")
    print("1. Hidden input (secure) - recommended")
    print("2. Visible input (less secure but works in all terminals)")
    
    while True:
        try:
            input_choice = input("Choose input method (1 or 2): ").strip()
            
            if input_choice == "1":
                print("\nNote: Your input will be hidden for security.")
                prompt = "Enter New Bearer Token: " if is_retry else "Enter Bearer Token: "
                
                try:
                    token = getpass.getpass(prompt).strip()
                except Exception as getpass_error:
                    print(f"❌ Hidden input failed: {getpass_error}")
                    print("Falling back to visible input...")
                    token = input(prompt).strip()
                    
            elif input_choice == "2":
                print("\n⚠️  WARNING: Token will be visible on screen!")
                confirm_visible = input("Continue with visible input? (y/n): ").lower().strip()
                if confirm_visible not in ['y', 'yes']:
                    continue
                    
                prompt = "Enter New Bearer Token: " if is_retry else "Enter Bearer Token: "
                token = input(prompt).strip()
            else:
                print("❌ Please choose 1 or 2")
                continue
                
            if not token:
                print("❌ Bearer token cannot be empty. Please try again.\n")
                continue
            
            # Basic validation - check if it looks like a JWT token
            if token.count('.') == 2:
                print("✅ Token format appears valid.")
                return token
            else:
                print("⚠️  Warning: Token doesn't appear to be in JWT format.")
                print(f"Token length: {len(token)} characters")
                print(f"First 20 chars: {token[:20]}...")
                confirm = input("Continue anyway? (y/n): ").lower().strip()
                if confirm in ['y', 'yes']:
                    return token
                print()
                
        except KeyboardInterrupt:
            print("\n❌ Input cancelled by user.")
            raise
        except Exception as e:
            print(f"❌ Error during token input: {e}")
            print("Please try again or use visible input method.")
            continue

def main():
    print("🎯 SECURE PATIENT DATA EXTRACTOR")
    print("=" * 50)
    print("This script is designed to be server-friendly:")
    print("✅ Secure token input (hidden from terminal)")
    print("✅ Saves all customer IDs first before processing")
    print("✅ Uses human-like randomized delays")
    print("✅ Automatic progress checkpoints")
    print("✅ Resume capability if interrupted")
    print("✅ Ultra-conservative rate limiting")
    print("🚨 NEW: Auto-creates CSV on ANY interruption/error")
    print("=" * 50)
    print()
    
    # Get bearer token securely
    try:
        print("🔄 Initializing bearer token input...")
        bearer_token = get_bearer_token()
        print("✅ Bearer token received successfully!")
        print(f"📏 Token length: {len(bearer_token)} characters")
        print("\n🚀 Starting extraction process...\n")
    except KeyboardInterrupt:
        print("\n\n❌ Process cancelled by user.")
        return
    except Exception as e:
        print(f"❌ Error getting bearer token: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Ultra-conservative configuration to avoid DDoS detection
    try:
        print("🔄 Initializing PatientDataExtractor...")
        extractor = PatientDataExtractor(
            bearer_token=bearer_token,
            requests_per_second=0.8,        # Less than 1 request per second
            max_retries=3,
            batch_size=20,                  # Small batches
            use_human_like_delays=True      # Randomized delays
        )
        print("✅ Extractor initialized successfully!")
        
        # Always use sequential processing for maximum safety
        print("🔄 Starting patient processing...")
        extractor.process_all_patients('patient_data.csv', use_threading=False)
        
        print("\n🎉 Script completed! Check the generated CSV file.")
        print("📁 Cache files created for recovery:")
        print("   - customer_ids_cache.json (for reusing customer IDs)")
        print("   - backup_patient_data.csv (backup CSV updated during processing)")
        print("   - progress_checkpoint.json (deleted after successful completion)")
        
    except KeyboardInterrupt:
        print("\n🚨 Script interrupted by user. CSV should be created automatically.")
    except Exception as e:
        print(f"\n❌ An error occurred during processing: {e}")
        print("💡 If the error is authentication-related, please check your bearer token.")
        print("🚨 Emergency CSV should be created automatically.")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clear token from memory for security
        if 'bearer_token' in locals():
            bearer_token = None
            print("🧹 Cleared bearer token from memory.")

if __name__ == "__main__":
    main()

# Recovery mode instructions:
"""
If the script gets interrupted, just run it again!
It will automatically:
1. Ask for your bearer token again (for security)
2. Ask if you want to use cached customer IDs
3. Ask if you want to resume from the last checkpoint
4. Continue where it left off

EMERGENCY CSV FEATURES:
- CSV is auto-created on ANY interruption (Ctrl+C, errors, crashes)
- backup_patient_data.csv is updated at every checkpoint
- Emergency CSV includes timestamp in filename
- All collected data is preserved even if script crashes

This ensures you never lose progress and don't have to re-fetch customer IDs!
"""