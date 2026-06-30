#Context: This code implements a debouncing mechanism using Redis and asyncio in Python. Debouncing is a technique used to limit the rate at which a function can be executed, ensuring that it is only called after a certain period of inactivity. This is particularly useful in scenarios where events may be triggered frequently, such as user input or network requests.
import asyncio
import json
import time
import logging
import redis.asyncio as redis

#Coroutine -> A coroutine is a special type of function in Python that can pause its execution and yield control back to the event loop, allowing other tasks to run concurrently. Coroutines are defined using the async def syntax and can be awaited using the await keyword. They are a fundamental part of asynchronous programming in Python, enabling efficient handling of I/O-bound operations without blocking the main thread.

#Creates logger object with the current file name
logger = logging.getLogger(__name__)
#Global Configuration for the logging file
#Which level logs will get printed
#What's the format of logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#logging.INFO -> This level will log all messages with severity level INFO and above (WARNING, ERROR, CRITICAL). It will not log DEBUG messages.

logger.info("Logging configuration set up successfully.")
logger.warning("This is a warning message.")
logger.error("This is an error message.")


class RedisAsyncDebouncer:
    #__init__() -> It's a special method in Python classes that is automatically called when a new instance of the class is created. It initializes the object's attributes and sets up any necessary state.
    def __init__(self, redis_url:str, wait_time:float, callback: callable):
        """
        redis_url: Connection string (ex: 'redis://localhost:6379/0')
        wait_time: Seconds to debounce
        callback: The async function to call after the debounce period
        """

        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.wait_time = wait_time
        self.callback = callback

#self -> when you create an instance of the class, self refers to that specific instance. It allows you to access the attributes and methods of the class within its own methods. so when we have to access the current instance of the class, we use self. It is a convention in Python to name the first parameter of instance methods as self, but you can technically use any name you like. However, using self is a widely accepted practice and makes your code more readable and understandable to other Python developers.

# (_) means that the method is private and should not be accessed from outside the class. It's a convention in Python to indicate that this method is intended for internal use within the class. You can still technically call it from outside the class, but it's a signal to other developers that they should avoid doing so. If they are calling they should know the risks of doing so. In this case, the method is responsible for waiting for the debounce period and then executing the callback if no new messages have arrived.

# (__) means name mangling. Python changes the name of the method to include the class name, making it harder to accidentally override or access from outside the class. It's a stronger indication that this method is intended for internal use only. In this case, the method is responsible for waiting for the debounce period and then executing the callback if no new messages have arrived. Example: If you have a class named MyClass and a method named __my_method, Python will internally rename it to _MyClass__my_method. This makes it less likely that subclasses or external code will accidentally override or access this method. 
    async def _wait_and_response(self, key:str):
        await asyncio.sleep(self.wait_time)

        target_key = f"debounce:target:{key}"
        payload_key = f"debounce:payload:{key}"
        lock_key=f"debounce:lock:{key}"

        try:
            #2. check if newwr message has arrived and updates the target time while er slept
            saved_target = await self.redis.get(target_key)
            current_time = time.time()

            #if the saved target time is still in the future, it means a new message has arrived and we should not execute the callback We must silently abort the execution of the callback function. This is the essence of debouncing: if a new event occurs before the wait time has elapsed, we cancel the previous action and reset the timer.
            if saved_target and float(saved_target) > current_time:
                logger.debug(f"Task for the {key} superseded by a new message. Not executing callback. Aborting")
                return
            
            #3. If the saved target time is in the past or doesn't exist, we can proceed to execute the callback function. Before doing so, we should acquire a lock to ensure that only one instance of the callback is executed for this key at a time
            #We survived! Acquire an atomic lock to ensure no other worker processes the same key at the same time. This is important in a distributed system where multiple instances of this debouncer might be running concurrently.
            lock_acquired = await self.redis.set(lock_key, "locked", nx = True, ex = 10)

            if not lock_acquired:
                logger.debug(f"Another worker is already processing the callback for {key}. Not executing callback. Aborting")
                return
            
            #4. fetch the 
            payload_str = await self.redis.get(payload_key)
            if(payload_str):
                payload = json.loads(payload_str)

                try:
                    logger.info(f"Executing debounced callback for {key} with payload: {payload}")
                    await self.callback(payload)
                except Exception as e:
                    logger.error(f"Error executing callback for {key}: {e}")
                finally:
                    #5. Clean up the keys in Redis after the callback has been executed, regardless of whether it succeeded or failed. This ensures that we don't leave stale data in Redis and that future events can be processed correctly.
                    await self.redis.delete(target_key, payload_key, lock_key) 
        except Exception as e:
            logger.error(f"Error in _wait_and_response for {key}: {e}", exc_info=True)
            #exc_info=True -> This argument tells the logger to include the traceback information in the log message. This is useful for debugging because it provides context about where the error occurred in the code, making it easier to identify and fix issues. When an exception is logged with exc_info=True, the log will include the stack trace, which shows the sequence of function calls that led to the error.

    async def trigger(self, key:str, payload:dict):
        """
        Call this method to trigger the debouncer with a specific key and payload. If the debouncer is already waiting for a previous event with the same key, it will reset the wait time and update the payload. If not, it will start a new wait period.
        Call it everytime when the event occurs. The debouncer will ensure that the callback is only executed after a period of inactivity defined by wait_time.
        """
        target_time = time.time() + self.wait_time

        #overwrite the payload and target time in Redis. This ensures that if a new event occurs before the wait time has elapsed, the previous payload and target time are updated, effectively resetting the debounce timer.
        await self.redis.set(f"debounce:payload:{key}", json.dumps(payload))
        await self.redis.set(f"debounce:target:{key}", str(target_time))

        #Fireoff the background task to wait and execute the callback after the debounce period. This is done asynchronously, allowing the main event loop to continue processing other tasks without being blocked by the wait time.
        asyncio.create_task(self._wait_and_response(key))

    async def close(self):
        """
        Close the Redis connection. This should be called when the debouncer is no longer needed to free up resources.
        """
        await self.redis.aclose()

    #Production usage example of the callback function that will be executed after the debounce period. In a real-world scenario, this function would contain the logic to save the payload to a database or perform any other necessary actions. The function is defined as async to allow for asynchronous operations, such as database writes, without blocking the event loop.
async def save_to_database(payload:dict):
    """
    Heavy Database logic goes here. This function simulates a database save operation by sleeping for 1 second. In a real application, this would be replaced with actual database interaction code.
    """    
    print(f"Saving to database: {payload['data']} for user {payload['user_id']}")

async def main():
    #Create an instance of the RedisAsyncDebouncer with a wait time of 2 seconds and the save_to_database callback function. This sets up the debouncer to listen for events and execute the callback after a period of inactivity.
    debouncer = RedisAsyncDebouncer(redis_url="redis://localhost:6379/0", wait_time=2, callback=save_to_database)   
    #__init__() -> method is called when the RedisAsyncDebouncer instance is created. It initializes the Redis connection, sets the wait time for debouncing, and assigns the callback function that will be executed after the debounce period.
        
    print("🚀 Stream receiving rapid, chaotic messages across multiple workers...")
    
    # Simulating messages arriving at different times (potentially hitting different servers)
    await debouncer.trigger("user_123", {"user_id": "user_123", "data": "Mouse click 1"})
    await asyncio.sleep(0.5)
    
    await debouncer.trigger("user_123", {"user_id": "user_123", "data": "Mouse click 2"})
    await asyncio.sleep(0.5)
    
    await debouncer.trigger("user_123", {"user_id": "user_123", "data": "Final valid click"})
    
        # Wait to let the debounce timer expire and process
    await asyncio.sleep(3.0)
    
    # Clean up Redis connection
    await debouncer.close()

#__name__ is speacial variable in Python that represents the name of the current module. When a Python file is run directly, __name__ is set to "__main__". This allows you to check if the script is being run directly or being imported as a module in another script. If it's run directly, the code block under if __name__ == "__main__": will execute. If it's imported, that block will not execute, allowing for modular code organization.
if __name__ == "__main__":
    asyncio.run(main()) 

#normally the main function is written as simple def main(). But in this it's async def main() because it contains asynchronous code that needs to be run within an event loop. The asyncio.run(main()) function is used to start the event loop and execute the main coroutine. This is necessary because the debouncer relies on asynchronous operations, such as waiting for a period of inactivity and interacting with Redis, which are best handled using asyncio's event-driven model.So main function is coroutine
                           


        