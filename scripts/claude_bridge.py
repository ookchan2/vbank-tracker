#!/usr/bin/env python3
"""
Claude Code Bridge for Autonomous Mode
This module provides a bridge between the vbank tracker and Claude Code's AI
When no API key is available, it writes tasks to files for Claude to process
"""

import os
import json
import time
from pathlib import Path
from datetime import datetime

BRIDGE_DIR = Path(__file__).parent.parent / 'data' / 'claude_bridge'
BRIDGE_DIR.mkdir(parents=True, exist_ok=True)

def is_autonomous_mode():
    """Check if we should use autonomous (Claude Code) mode"""
    api_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()
    return not api_key or api_key == 'autonomous-mode'

def request_ai_analysis(task_type, content, metadata=None):
    """
    Request AI analysis from Claude Code

    Args:
        task_type: 'extract_promotions', 'extract_products', 'dedup', 'match', 'insights'
        content: The content to analyze
        metadata: Additional context (bank_name, etc.)

    Returns:
        Analysis result (will block until Claude responds)
    """
    request_id = f"{task_type}_{int(time.time())}"
    request_file = BRIDGE_DIR / f"request_{request_id}.json"
    response_file = BRIDGE_DIR / f"response_{request_id}.json"

    # Write request
    request = {
        'id': request_id,
        'type': task_type,
        'content': content,
        'metadata': metadata or {},
        'timestamp': datetime.now().isoformat(),
        'status': 'pending'
    }

    with open(request_file, 'w', encoding='utf-8') as f:
        json.dump(request, f, indent=2, ensure_ascii=False)

    print(f"  [BRIDGE] AI analysis requested: {task_type}")
    print(f"  [BRIDGE] Request written to: {request_file}")
    print(f"  [BRIDGE] Waiting for Claude Code to process...")
    print(f"  [BRIDGE] (In autonomous mode, Claude will read and respond)")
    print()

    # In a real autonomous implementation, we would:
    # 1. Pause here and wait for Claude Code to process
    # 2. Claude reads the request file
    # 3. Claude writes the response file
    # 4. We continue

    # For now, return a placeholder that indicates manual processing needed
    return {
        'status': 'manual_processing_required',
        'request_file': str(request_file),
        'message': 'In autonomous mode, Claude Code needs to process this request manually'
    }

def check_for_response(request_id, timeout=300):
    """Check if Claude has responded to a request"""
    response_file = BRIDGE_DIR / f"response_{request_id}.json"

    start = time.time()
    while time.time() - start < timeout:
        if response_file.exists():
            with open(response_file, 'r', encoding='utf-8') as f:
                response = json.load(f)
            # Clean up
            request_file = BRIDGE_DIR / f"request_{request_id}.json"
            if request_file.exists():
                request_file.unlink()
            response_file.unlink()
            return response.get('result')

        time.sleep(1)

    return None

# Export functions
__all__ = ['is_autonomous_mode', 'request_ai_analysis', 'check_for_response']
