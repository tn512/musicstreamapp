import json
import time
import random
import logging
import argparse
import socket
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any

import pandas as pd
import numpy as np
from faker import Faker
from kafka import KafkaProducer
from kafka.client_async import KafkaClient
from kafka.cluster import ClusterMetadata

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG for more verbose logging
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Enable debug logging for kafka
logging.getLogger('kafka').setLevel(logging.DEBUG)

def json_serial(obj: Any) -> Any:
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    if isinstance(obj, (np.float64, np.float32, np.float16)):
        return float(obj)
    raise TypeError(f'Type {type(obj)} not serializable')

class ExternalBrokerMetadata(ClusterMetadata):
    def __init__(self, external_broker: str):
        super().__init__()
        self.external_broker = external_broker

    def brokers(self):
        """Override to always return the external broker."""
        return {0: (self.external_broker.split(':')[0], int(self.external_broker.split(':')[1]))}

class ExternalBrokerClient(KafkaClient):
    def __init__(self, external_broker: str, **configs):
        self.external_broker = external_broker
        super().__init__(**configs)

    def _get_metadata(self):
        """Override to use external broker metadata."""
        return ExternalBrokerMetadata(self.external_broker)

# Page transition probabilities
PAGE_TRANSITIONS = {
    "Home": {
        "NextSong": 0.7,
        "Logout": 0.05,
        "Home": 0.1,
        "About": 0.05,
        "Settings": 0.05,
        "Error": 0.001
    },
    "NextSong": {
        "NextSong": 0.8,
        "Home": 0.1,
        "Logout": 0.05,
        "Settings": 0.03,
        "Error": 0.001
    },
    "Settings": {
        "NextSong": 0.4,
        "Home": 0.4,
        "Logout": 0.1,
        "Settings": 0.08,
        "Error": 0.001
    },
    "Error": {
        "Home": 0.5,
        "NextSong": 0.3,
        "Settings": 0.1,
        "Error": 0.001
    },
    "Logout": {
        "Login": 0.999,
        "Error": 0.001
    },
    "Login": {
        "Home": 0.9,
        "Settings": 0.09,
        "Error": 0.001
    }
}

# Status change types and their probabilities in Settings page
STATUS_CHANGES = {
    "subscription_change": 0.4,
    "profile_update": 0.4,
    "settings_change": 0.2
}

class MusicEventGenerator:
    def __init__(self, kafka_bootstrap_servers: str = None):
        """Initialize the event generator with Kafka configuration."""
        self.fake = Faker()
        
        # Initialize Kafka producer if bootstrap servers are provided
        self.producer = None
        if kafka_bootstrap_servers:
            logger.info(f"Initializing Kafka producer with bootstrap servers: {kafka_bootstrap_servers}")
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=kafka_bootstrap_servers,
                    value_serializer=lambda x: json.dumps(x, default=json_serial).encode('utf-8'),
                    security_protocol="PLAINTEXT",
                    api_version=(2, 0, 0),
                    client_id="event-generator",
                    connections_max_idle_ms=10000,
                    request_timeout_ms=30000,
                    retry_backoff_ms=500,
                    reconnect_backoff_ms=1000,
                    reconnect_backoff_max_ms=10000
                )
                logger.info("Kafka producer initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka producer: {str(e)}")
                self.producer = None
        else:
            logger.warning("No Kafka bootstrap servers provided. Events will be logged locally only.")
        
        # Load song data
        logger.info("Loading song data...")
        self.songs_df = pd.read_csv('data/songs_analysis.txt.gz', 
                                   compression='gzip', 
                                   sep='\t',
                                   header=None,
                                   names=['sample_rate', 'song_id', 'start_of_fade_out', 'duration',
                                         'loudness', 'end_of_fade_in'] + [f'key_{i}' for i in range(12)] +
                                         ['mode_0', 'mode_1', 'time_signature_0', 'time_signature_1',
                                         'loudness_max', 'loudness_max_time', 'loudness_start', 'tempo',
                                         'time_signature_confidence', 'time_signature',
                                         'mode_confidence', 'track_id'])
        
        # Load song metadata (artist, title) from listen_counts
        logger.info("Loading song metadata...")
        self.song_metadata = pd.read_csv('data/listen_counts.txt.gz',
                                       compression='gzip',
                                       sep='\t',
                                       header=None,
                                       names=['track_id', 'artist_name', 'title', 'duration', 'listen_count'])
        
        # Convert track_id to string in both dataframes
        self.songs_df['track_id'] = self.songs_df['track_id'].astype(str)
        self.song_metadata['track_id'] = self.song_metadata['track_id'].astype(str)
        
        # Merge song data with metadata
        self.songs_df = pd.merge(self.songs_df, self.song_metadata[['track_id', 'artist_name', 'title']], 
                                on='track_id', how='inner')  # Change to inner join to ensure we have metadata
        
        logger.info(f"Loaded {len(self.songs_df)} songs after merging")
        
        # Convert duration to milliseconds
        self.songs_df['duration'] = (self.songs_df['duration'] * 1000).astype(int)
        
        # Load user agents
        logger.info("Loading user agents...")
        try:
            # Load user agents from CSV file
            user_agents_df = pd.read_csv('data/user_agent.csv')
            self.user_agents = user_agents_df['useragent'].tolist()  # Using the correct column name from CSV
            
            if not self.user_agents:  # If no user agents loaded, use some defaults
                self.user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X)",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    "Mozilla/5.0 (Linux; Android 11; Pixel 5)"
                ]
            
            logger.info(f"Loaded {len(self.user_agents)} user agents")
        except Exception as e:
            logger.error(f"Error loading user agents from CSV: {e}")
            # Use defaults
            self.user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Mozilla/5.0 (Linux; Android 11; Pixel 5)"
            ]
            logger.info("Using default user agents")
        
        # Initialize user session data
        self.active_sessions: Dict[str, Dict] = {}
        
    def _get_base_event_data(self, user_id: str, session_id: str, 
                            item_in_session: int, auth_status: str = "Logged In",
                            level: str = "paid") -> Dict:
        """Generate base event data common to all event types."""
        current_time = int(time.time() * 1000)
        return {
            "ts": current_time,
            "sessionId": session_id,
            "userId": user_id,
            "auth": auth_status,
            "level": level,
            "itemInSession": item_in_session,
            "city": self.fake.city(),
            "zip": self.fake.zipcode(),
            "state": self.fake.state_abbr(),
            "userAgent": random.choice(self.user_agents),
            "lon": float(self.fake.longitude()),
            "lat": float(self.fake.latitude()),
            "lastName": self.fake.last_name(),
            "firstName": self.fake.first_name(),
            "gender": random.choice(['M', 'F']),
            "registration": current_time - random.randint(0, 90*24*60*60*1000)
        }
    
    def generate_user_id(self) -> str:
        """Generate a unique user ID."""
        return str(self.fake.random_int(min=1, max=1000000))
    
    def generate_session_id(self) -> str:
        """Generate a unique session ID."""
        return str(self.fake.random_int(min=1, max=1000000))
    
    def get_next_page(self, current_page: str) -> str:
        """Determine the next page based on transition probabilities."""
        transitions = PAGE_TRANSITIONS.get(current_page, PAGE_TRANSITIONS["Home"])
        return random.choices(
            list(transitions.keys()),
            weights=list(transitions.values()),
            k=1
        )[0]
    
    def get_method_and_status(self, page: str, auth_success: bool = True) -> Tuple[str, int]:
        """Get HTTP method and status code for a page."""
        if page == "NextSong":
            return "PUT", 200
        elif page in ["Logout", "Login"]:
            return "PUT", 307 if auth_success else 401
        elif page == "Error":
            return "GET", 404
        else:
            return "GET", 200
    
    def generate_listen_event(self, user_id: str, session_id: str, item_in_session: int) -> Dict:
        """Generate a song listening event."""
        song = self.songs_df.sample(n=1).iloc[0]
        event = self._get_base_event_data(user_id, session_id, item_in_session)
        
        event.update({
            "artist": song["artist_name"],
            "song": song["title"],
            "duration": song["duration"]
        })
        
        return event
    
    def generate_page_view_event(self, user_id: str, session_id: str, page: str, 
                               item_in_session: int, auth_status: str = "Logged In") -> Dict:
        """Generate a page view event."""
        method, status = self.get_method_and_status(page)
        event = self._get_base_event_data(user_id, session_id, item_in_session, auth_status)
        
        event.update({
            "page": page,
            "method": method,
            "status": status
        })
        
        return event
    
    def generate_auth_event(self, user_id: str, session_id: str, item_in_session: int,
                          auth_type: str, success: bool = True) -> Dict:
        """Generate an authentication event."""
        method, status = self.get_method_and_status(auth_type, success)
        event = self._get_base_event_data(
            user_id, session_id, item_in_session,
            "Logged In" if auth_type == "Login" and success else "Logged Out"
        )
        
        event.update({
            "success": success,
            "method": method,
            "status": status
        })
        
        return event
    
    def generate_status_change_event(self, user_id: str, session_id: str, 
                                   item_in_session: int, prev_level: str = "free") -> Dict:
        """Generate a status change event."""
        event = self._get_base_event_data(user_id, session_id, item_in_session)
        
        status_change_type = random.choices(
            list(STATUS_CHANGES.keys()),
            weights=list(STATUS_CHANGES.values()),
            k=1
        )[0]
        
        event.update({
            "prevLevel": prev_level,
            "method": "PUT",
            "status": 200,
            "statusChangeType": status_change_type
        })
        
        return event
    
    def simulate_user_session(self):
        """Simulate a user session with multiple events."""
        user_id = self.generate_user_id()
        session_id = self.generate_session_id()
        current_page = "Login"
        item_in_session = 0
        auth_status = "Logged Out"
        current_level = "free"
        
        # Generate initial login auth event
        auth_success = random.random() < 0.95  # 95% success rate
        auth_event = self.generate_auth_event(
            user_id, session_id, item_in_session,
            "Login", auth_success
        )
        self.send_event('auth_events', auth_event)
        item_in_session += 1
        
        if not auth_success:
            return
        
        auth_status = "Logged In"
        
        while True:
            # Generate page view event
            page_event = self.generate_page_view_event(
                user_id, session_id, current_page, 
                item_in_session, auth_status
            )
            self.send_event('page_view_events', page_event)
            item_in_session += 1
            
            # Handle special pages
            if current_page == "Settings":
                # 30% chance of status change in Settings
                if random.random() < 0.3:
                    status_event = self.generate_status_change_event(
                        user_id, session_id, item_in_session,
                        prev_level=current_level
                    )
                    self.send_event('status_change_events', status_event)
                    item_in_session += 1
                    
                    # Update level if it was a subscription change
                    if status_event['statusChangeType'] == 'subscription_change':
                        current_level = "paid" if current_level == "free" else "free"
            
            elif current_page == "NextSong":
                listen_event = self.generate_listen_event(
                    user_id, session_id, item_in_session
                )
                self.send_event('listen_events', listen_event)
                item_in_session += 1
                
                # Simulate listening duration
                time.sleep(min(listen_event['duration'] / 20000, 3))
            
            elif current_page == "Logout":
                # Generate logout auth event
                auth_event = self.generate_auth_event(
                    user_id, session_id, item_in_session,
                    "Logout", True
                )
                self.send_event('auth_events', auth_event)
                break
            
            # Get next page
            current_page = self.get_next_page(current_page)
            
            # Small delay between page views
            time.sleep(random.uniform(0.1, 0.5))

    def send_event(self, topic: str, event: Dict[str, Any]) -> None:
        """Send an event to a Kafka topic."""
        if self.producer:
            try:
                # Log the event being sent
                logger.info(f"Sending event to Kafka topic {topic}")
                
                # Simplified retry logic for single broker setup
                max_retries = 2
                retry_count = 0
                success = False
                
                while not success and retry_count < max_retries:
                    try:
                        future = self.producer.send(topic, event)
                        record_metadata = future.get(timeout=10)
                        logger.info(f"Successfully delivered message to Kafka topic={record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}")
                        success = True
                    except Exception as e:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"Error sending message, retrying ({retry_count}/{max_retries}): {str(e)}")
                            # Brief delay before retry
                            time.sleep(1)
                        else:
                            logger.error(f"Failed to send event after {max_retries} retries: {str(e)}")
                            logger.error(f"Event data: {json.dumps(event, default=json_serial)[:200]}...")
                
            except Exception as e:
                logger.error(f"Failed to send event to Kafka topic {topic}: {str(e)}")
                logger.error(f"Event data: {json.dumps(event, default=json_serial)[:200]}...")  # Log truncated event data
        else:
            logger.warning(f"Kafka producer not available. Event for topic {topic} not sent.")

def main():
    """Main function to run the event generator."""
    parser = argparse.ArgumentParser(description="Music Event Generator")
    parser.add_argument("--kafka-bootstrap-servers", type=str, help="Kafka bootstrap servers")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.info("Debug logging enabled")
    
    # Log the bootstrap servers for debugging
    if args.kafka_bootstrap_servers:
        logger.info(f"Using Kafka bootstrap servers: {args.kafka_bootstrap_servers}")
        
        # Extract host and port for direct connection testing
        bootstrap_parts = args.kafka_bootstrap_servers.split(':')
        if len(bootstrap_parts) >= 2:
            kafka_host = bootstrap_parts[0]
            kafka_port = bootstrap_parts[1]
            logger.info(f"Using Kafka host: {kafka_host}, port: {kafka_port}")
            
            # Test basic network connectivity to Kafka broker
            try:
                logger.info(f"Testing basic TCP connection to Kafka broker at {kafka_host}:{kafka_port}...")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((kafka_host, int(kafka_port)))
                s.close()
                logger.info(f"TCP connection test successful")
            except Exception as e:
                logger.error(f"TCP connection test failed: {str(e)}")
    else:
        logger.warning("No Kafka bootstrap servers provided. Events will be logged locally only.")
    
    # Initialize the generator with limited retries
    max_retries = 3
    retry_count = 0
    generator = None
    
    while retry_count < max_retries:
        try:
            generator = MusicEventGenerator(args.kafka_bootstrap_servers)
            
            # Test Kafka connectivity if a producer was created
            if generator.producer:
                try:
                    logger.info("Testing Kafka producer with a test message...")
                    test_event = {"message": "Test event", "timestamp": int(time.time() * 1000)}
                    future = generator.producer.send("test_events", test_event)
                    record_metadata = future.get(timeout=10)
                    logger.info(f"Test message sent successfully. Topic={record_metadata.topic}, Partition={record_metadata.partition}")
                    break  # Successfully connected to Kafka
                except Exception as e:
                    logger.error(f"Test message failed: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 3 * retry_count
                        logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to connect to Kafka after {max_retries} attempts.")
                        break
            else:
                # No producer was created, so no need to retry
                break
        except Exception as e:
            logger.error(f"Error initializing event generator: {e}")
            retry_count += 1
            if retry_count < max_retries:
                wait_time = 3 * retry_count
                logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to initialize event generator after {max_retries} attempts.")
                return
    
    if not generator:
        logger.error("Failed to initialize event generator. Exiting.")
        return
    
    logger.info("Starting event generation...")
    
    try:
        while True:
            try:
                # Generate session
                generator.simulate_user_session()
                
            except Exception as e:
                logger.error(f"Error in session generation: {e}")
                time.sleep(5)  # Wait before retrying
                
    except KeyboardInterrupt:
        logger.info("Stopping event generation...")

if __name__ == "__main__":
    main() 