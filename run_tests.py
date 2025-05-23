import pytest
import os
import sys

def run_tests():
    """Run the tests for the video extractor API"""
    # Set environment variables for testing if needed
    os.environ["TESTING"] = "True"
    
    # Run the tests with warning filters
    # -xvs: exit on first failure, verbose, don't capture output
    # --disable-warnings: disable warning summary
    # -p no:warnings: disable warnings plugin
    exit_code = pytest.main(['-xvs', '--disable-warnings', '-p', 'no:warnings', 'tests/test_api.py'])
    
    return exit_code

if __name__ == "__main__":
    sys.exit(run_tests())
