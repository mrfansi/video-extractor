"""Patches for third-party libraries to fix deprecation warnings."""

import datetime
import functools
import importlib.util
import warnings

from loguru import logger


def apply_patches():
    """Apply patches to third-party libraries to fix deprecation warnings."""
    # Filter warnings for botocore utcnow deprecation
    warnings.filterwarnings("ignore", message="datetime.datetime.utcnow\\(\\) is deprecated", module="botocore")
    logger.info("Added filter for botocore.auth.datetime.utcnow() deprecation warning")
    
    # Patch botocore.auth to use datetime.UTC instead of utcnow()
    try:
        patch_botocore_utcnow()
        logger.info("Applied patch for botocore.auth.datetime.utcnow() deprecation warning")
    except Exception as e:
        logger.warning(f"Failed to apply botocore patch: {e}")


def patch_botocore_utcnow():
    """Patch botocore.auth to use datetime.UTC instead of utcnow()."""
    # Check if botocore is installed
    if not importlib.util.find_spec("botocore"):
        logger.warning("botocore is not installed, skipping patch")
        return
    
    # Import botocore modules that use utcnow
    import botocore.auth
    import botocore.credentials
    import botocore.signers
    import botocore.tokens
    import botocore.utils
    
    # Define the patched function
    @functools.wraps(datetime.datetime.utcnow)
    def patched_utcnow():
        """Patched version of datetime.utcnow() that uses datetime.now(UTC)."""
        return datetime.datetime.now(datetime.UTC)
    
    # Apply the patch to all relevant modules
    modules_to_patch = [
        botocore.auth,
        botocore.credentials,
        botocore.signers,
        botocore.tokens,
        botocore.utils
    ]
    
    # Replace datetime.utcnow with our patched version in each module
    for module in modules_to_patch:
        if hasattr(module, 'datetime') and hasattr(module.datetime, 'datetime'):
            module.datetime.datetime.utcnow = patched_utcnow
        elif hasattr(module, 'datetime'):
            module.datetime.utcnow = patched_utcnow
            
    # Also patch the direct import if present
    if hasattr(botocore.auth, 'datetime'):
        botocore.auth.datetime.datetime.utcnow = patched_utcnow
