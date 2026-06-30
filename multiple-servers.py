import asyncio
import json
import time
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)

# CHANGED TO DEBUG: So we can see the lock battles and superseded tasks!
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class RedisAsyncDebouncer:
    def __init__(self, redis_url:str, wait_time:float, callback: callable):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.wait_time = wait_time
        self.callback = callback

    async def _wait_and_response(self, key:str):
        await asyncio.sleep(self.wait_time)

        target_key = f"debounce:target:{key}"
        payload_key = f"debounce:payload:{key}"
        lock_key=f"debounce:lock:{key}"

        try:
            saved_target = await self.redis.get(target_key)
            current_time = time.time()

            if saved_target and float(saved_target) > current_time:
                logger.debug(f"Task for {key} superseded by a new message. Aborting.")
                return
            
            # The Race Condition Happens Here!
            # Multiple asleep tasks wake up and try to grab this exact lock.
            lock_acquired = await self.redis.set(lock_key, "locked", nx=True, ex=10)

            if not lock_acquired:
                logger.debug(f"LOCK DENIED: Another worker is processing {key}. Aborting.")
                return
            
            payload_str = await self.redis.get(payload_key)
            if payload_str:
                payload = json.loads(payload_str)

                try:
                    logger.info(f"Executing debounced callback for {key} with payload: {payload}")
                    await self.callback(payload)
                except Exception as e:
                    logger.error(f"Error executing callback for {key}: {e}")
                finally:
                    await self.redis.delete(target_key, payload_key, lock_key) 
        except Exception as e:
            logger.error(f"Error in _wait_and_response for {key}: {e}", exc_info=True)

    async def trigger(self, key:str, payload:dict):
        target_time = time.time() + self.wait_time

        await self.redis.set(f"debounce:payload:{key}", json.dumps(payload))
        await self.redis.set(f"debounce:target:{key}", str(target_time))

        asyncio.create_task(self._wait_and_response(key))

    async def close(self):
        await self.redis.aclose()


async def save_to_database(payload:dict):
    """Simulates database save."""    
    print(f"\n [DATABASE WRITE] Saving data: '{payload['data']}' handled by {payload['server_name']}\n")


async def run_server_instance(server_name: str, debouncer: RedisAsyncDebouncer, events: list):
    """
    Simulates a single server receiving requests over time.
    events is a list of tuples: (delay_in_seconds, message_data)
    """
    logger.info(f" {server_name} booted up and listening for traffic...")
    
    for delay, data in events:
        await asyncio.sleep(delay)
        payload = {
            "user_id": "user_123", 
            "data": data, 
            "server_name": server_name # Track which server caught the final payload
        }
        logger.info(f"[{server_name}] Received event '{data}' -> Triggering debouncer")
        await debouncer.trigger("user_123", payload)


async def main():
    debouncer = RedisAsyncDebouncer(redis_url="redis://localhost:6379/0", wait_time=2.0, callback=save_to_database)   
        
    print("Stream receiving rapid, chaotic messages across multiple simulated servers...\n")
    
    # Define the chaos: 3 different servers receiving clicks at overlapping times.
    # Format: (seconds_to_wait_before_firing, message_string)
    server_1_events = [
        (0.1, "Mouse click 1"),
        (0.5, "Mouse click 2"),
    ]
    
    server_2_events = [
        (0.3, "Keyboard smash"),
        (0.8, "Frantic clicking"),
    ]
    
    server_3_events = [
        (1.2, "Final calm click"), 
    ]
    
    # Spin up all 3 servers concurrently
    task1 = run_server_instance("Server-A", debouncer, server_1_events)
    task2 = run_server_instance("Server-B", debouncer, server_2_events)
    task3 = run_server_instance("Server-C", debouncer, server_3_events)
    
    # Run the simulation
    await asyncio.gather(task1, task2, task3)
    
    # The last event fires at 1.2 seconds. The debounce wait is 2.0 seconds.
    # We need to wait at least 3.2 seconds for the final callback to trigger.
    logger.info("All servers finished receiving traffic. Waiting for debounce timer to settle...")
    await asyncio.sleep(4.0)
    
    await debouncer.close()
    logger.info("Simulation complete. Redis connection closed.")

if __name__ == "__main__":
    asyncio.run(main())