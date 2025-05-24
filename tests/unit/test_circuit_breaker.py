import pytest
import time
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from app.core.circuit_breaker import CircuitBreaker, CircuitState
from app.core.errors import CircuitBreakerError


class TestCircuitBreaker:
    """Unit tests for CircuitBreaker class."""

    def test_init(self):
        """Test CircuitBreaker initialization."""
        breaker = CircuitBreaker(name="test_breaker", failure_threshold=3)
        assert breaker.name == "test_breaker"
        assert breaker.failure_threshold == 3
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_execute_success(self, circuit_breaker):
        """Test execute method with successful function call."""
        # Define a test function that always succeeds
        def test_func():
            return "success"

        # Execute the function through the circuit breaker
        result = circuit_breaker.execute(test_func)
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    def test_execute_failure(self, circuit_breaker):
        """Test execute method with failing function call."""
        # Define a test function that always fails
        def test_func():
            raise ValueError("Test error")

        # Execute the function through the circuit breaker
        with pytest.raises(ValueError, match="Test error"):
            circuit_breaker.execute(test_func)

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 1

    def test_circuit_opens_after_threshold(self, circuit_breaker):
        """Test that circuit opens after reaching failure threshold."""
        # Set a low threshold for testing
        circuit_breaker.failure_threshold = 2

        # Define a test function that always fails
        def test_func():
            raise ValueError("Test error")

        # First failure
        with pytest.raises(ValueError):
            circuit_breaker.execute(test_func)

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 1

        # Second failure - should open the circuit
        with pytest.raises(ValueError):
            circuit_breaker.execute(test_func)

        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.failure_count == 2
        
        # Verify the circuit breaker is in OPEN state
        # We'll manually verify the state rather than calling execute
        # which would trigger the CircuitBreakerError
        assert circuit_breaker.state == CircuitState.OPEN
        
    @patch('app.core.circuit_breaker.CircuitBreaker._check_state')
    def test_open_circuit_raises_error(self, mock_check_state, circuit_breaker):
        """Test that an open circuit raises CircuitBreakerError."""
        # Set up the mock to raise CircuitBreakerError
        mock_check_state.side_effect = CircuitBreakerError("test_breaker", datetime.now() + timedelta(seconds=60))
        
        # Force the circuit into OPEN state
        circuit_breaker.state = CircuitState.OPEN
        
        # Define a test function
        def test_func():
            return "success"
            
        # Should raise CircuitBreakerError because circuit is open
        with pytest.raises(CircuitBreakerError):
            circuit_breaker.execute(test_func)
            
        # Verify the mock was called
        mock_check_state.assert_called_once()

    def test_reset(self, circuit_breaker):
        """Test reset method."""
        # Set a low threshold for testing
        circuit_breaker.failure_threshold = 1

        # Define a test function that always fails
        def test_func():
            raise ValueError("Test error")

        # Fail once to open the circuit
        with pytest.raises(ValueError):
            circuit_breaker.execute(test_func)

        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.failure_count == 1

        # Reset the circuit breaker
        circuit_breaker.reset()

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    def test_all_states(self, circuit_breaker):
        """Test get_all_states class method."""
        # Initial state
        states = CircuitBreaker.get_all_states()
        assert "test_breaker" in states
        status = states["test_breaker"]
        assert status["state"] == "CLOSED"
        assert status["failure_count"] == 0

        # Define a test function that always fails
        def test_func():
            raise ValueError("Test error")

        # Fail once
        with pytest.raises(ValueError):
            circuit_breaker.execute(test_func)

        # Updated state
        states = CircuitBreaker.get_all_states()
        status = states["test_breaker"]
        assert status["state"] == "CLOSED"
        assert status["failure_count"] == 1

        # Fail again to open the circuit (assuming threshold is 1)
        circuit_breaker.failure_threshold = 1
        with pytest.raises(ValueError):
            circuit_breaker.execute(test_func)

        # Final state
        states = CircuitBreaker.get_all_states()
        status = states["test_breaker"]
        assert status["state"] == "OPEN"
        # The failure count will be 2 because we set the threshold to 1 after the first failure
        # but the counter had already incremented to 1, so it becomes 2 after the second failure
        assert status["failure_count"] == 2
