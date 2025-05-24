import time
import random
from enum import Enum
from typing import Callable, Optional, TypeVar, Any, Dict, Union
from functools import wraps
from datetime import datetime, timedelta
from loguru import logger


T = TypeVar('T')


class CircuitState(Enum):
    """Enum representing the possible states of a circuit breaker."""
    CLOSED = 'CLOSED'  # Normal operation, requests pass through
    OPEN = 'OPEN'      # Circuit is open, requests fail fast
    HALF_OPEN = 'HALF_OPEN'  # Testing if the service is back to normal


class CircuitBreakerError(Exception):
    """Exception raised when a circuit breaker is open."""
    
    def __init__(self, service_name: str, open_until: datetime):
        self.service_name = service_name
        self.open_until = open_until
        time_remaining = (open_until - datetime.now()).total_seconds()
        self.message = f"Circuit breaker for {service_name} is open. Will try again in {time_remaining:.2f} seconds."
        super().__init__(self.message)


class CircuitBreaker:
    """Implementation of the Circuit Breaker pattern.
    
    The circuit breaker monitors failures in service calls and prevents
    cascading failures by failing fast when a service is experiencing problems.
    
    Attributes:
        name: Name of the service being protected
        failure_threshold: Number of failures before opening the circuit
        reset_timeout: Time in seconds to wait before trying again (half-open state)
        half_open_max_calls: Maximum number of calls to allow in half-open state
        exclude_exceptions: List of exception types that should not count as failures
        state: Current state of the circuit breaker
        failure_count: Current count of consecutive failures
        last_failure_time: Time of the last failure
        next_attempt_time: Time when the circuit will transition to half-open
        half_open_calls: Number of calls made while in half-open state
        success_count: Number of consecutive successful calls in half-open state
    """
    
    # Class-level dictionary to store all circuit breaker instances
    _instances: Dict[str, 'CircuitBreaker'] = {}
    
    @classmethod
    def get_instance(cls, name: str) -> 'CircuitBreaker':
        """Get or create a circuit breaker instance by name."""
        if name not in cls._instances:
            cls._instances[name] = CircuitBreaker(name)
        return cls._instances[name]
    
    @classmethod
    def get_all_states(cls) -> Dict[str, Dict[str, Union[str, int, float]]]:
        """Get the states of all circuit breakers."""
        states = {}
        for name, instance in cls._instances.items():
            next_attempt = None
            if instance.next_attempt_time:
                next_attempt = (instance.next_attempt_time - datetime.now()).total_seconds()
                
            states[name] = {
                'state': instance.state.value,
                'failure_count': instance.failure_count,
                'next_attempt_in_seconds': next_attempt,
                'half_open_calls': instance.half_open_calls,
                'success_count': instance.success_count
            }
        return states
    
    def __init__(self, name: str, failure_threshold: int = 5, 
                 reset_timeout: int = 60, half_open_max_calls: int = 3,
                 exclude_exceptions: Optional[list] = None):
        """Initialize a new circuit breaker."""
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_calls = half_open_max_calls
        self.exclude_exceptions = exclude_exceptions or []
        
        # State tracking
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.next_attempt_time: Optional[datetime] = None
        self.half_open_calls = 0
        self.success_count = 0
        
        # Add to class instances
        CircuitBreaker._instances[name] = self
        
        logger.info(f"Circuit breaker initialized for {name} with threshold {failure_threshold}")
    
    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to wrap a function with circuit breaker functionality."""
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return self.execute(lambda: func(*args, **kwargs))
        return wrapper
    
    def execute(self, func: Callable[[], T]) -> T:
        """Execute a function with circuit breaker protection."""
        self._check_state()
        
        try:
            result = func()
            self._on_success()
            return result
        except tuple(self.exclude_exceptions) as e:
            # These exceptions don't count as failures
            logger.debug(f"Excluded exception in {self.name}: {str(e)}")
            raise
        except Exception as e:
            self._on_failure(e)
            raise
    
    def _check_state(self) -> None:
        """Check and possibly update the current state of the circuit breaker."""
        now = datetime.now()
        
        if self.state == CircuitState.OPEN:
            if now >= self.next_attempt_time:
                logger.info(f"Circuit breaker for {self.name} transitioning from OPEN to HALF_OPEN")
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                self.success_count = 0
            else:
                # Still open, fail fast
                raise CircuitBreakerError(self.name, self.next_attempt_time)
        
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls >= self.half_open_max_calls:
                # Too many half-open calls, back to open
                logger.warning(f"Too many calls in HALF_OPEN state for {self.name}, returning to OPEN")
                self._trip()
                raise CircuitBreakerError(self.name, self.next_attempt_time)
            
            # Increment the call counter for half-open state
            self.half_open_calls += 1
    
    def _on_success(self) -> None:
        """Handle a successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            
            # If we've had enough successes, close the circuit
            if self.success_count >= self.half_open_max_calls:
                logger.info(f"Circuit breaker for {self.name} closing after successful tests")
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.half_open_calls = 0
                self.success_count = 0
        elif self.state == CircuitState.CLOSED:
            # Reset failure count on success in closed state
            self.failure_count = 0
    
    def _on_failure(self, exception: Exception) -> None:
        """Handle a failed call."""
        self.last_failure_time = datetime.now()
        
        if self.state == CircuitState.CLOSED:
            self.failure_count += 1
            logger.warning(f"Failure in {self.name}: {str(exception)}. Failure count: {self.failure_count}/{self.failure_threshold}")
            
            if self.failure_count >= self.failure_threshold:
                self._trip()
        
        elif self.state == CircuitState.HALF_OPEN:
            # Any failure in half-open state trips the circuit again
            logger.warning(f"Failure during HALF_OPEN state for {self.name}: {str(exception)}")
            self._trip()
    
    def _trip(self) -> None:
        """Trip the circuit breaker to OPEN state."""
        self.state = CircuitState.OPEN
        self.next_attempt_time = datetime.now() + timedelta(seconds=self.reset_timeout)
        
        # Add jitter to prevent thundering herd problem (all retries happening at once)
        jitter = random.uniform(0, self.reset_timeout * 0.1)  # 10% jitter
        self.next_attempt_time += timedelta(seconds=jitter)
        
        logger.warning(
            f"Circuit breaker for {self.name} tripped. Open until: {self.next_attempt_time} "
            f"({self.reset_timeout + jitter:.2f} seconds)"
        )
    
    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        previous_state = self.state
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0
        self.success_count = 0
        self.next_attempt_time = None
        
        logger.info(f"Circuit breaker for {self.name} manually reset from {previous_state.value} to CLOSED")


def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    reset_timeout: int = 60,
    half_open_max_calls: int = 3,
    exclude_exceptions: Optional[list] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator factory for circuit breaker pattern.
    
    Args:
        name: Name of the service being protected
        failure_threshold: Number of failures before opening the circuit
        reset_timeout: Time in seconds to wait before trying again
        half_open_max_calls: Maximum number of calls to allow in half-open state
        exclude_exceptions: List of exception types that should not count as failures
        
    Returns:
        A decorator function that applies circuit breaker logic
    """
    breaker = CircuitBreaker.get_instance(name)
    breaker.failure_threshold = failure_threshold
    breaker.reset_timeout = reset_timeout
    breaker.half_open_max_calls = half_open_max_calls
    breaker.exclude_exceptions = exclude_exceptions or []
    
    return breaker
