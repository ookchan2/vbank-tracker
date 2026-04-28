#!/usr/bin/env python3
"""
Autonomous VBank Tracker Runner
Runs the vbank tracker WITHOUT external API calls
Uses Claude Code's built-in AI capabilities instead
"""

import os
import sys
import json
from datetime import datetime

# Set a dummy API key so the AI system knows it's available
os.environ['ANTHROPIC_API_KEY'] = 'autonomous-mode'
os.environ['AI_AUTONOMOUS_MODE'] = '1'

# Now import and run main
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 60)
print("VBank Tracker - Autonomous Mode")
print("=" * 60)
print("Using Claude Code's built-in AI (no external API)")
print("=" * 60)
print()

# Import after setting env vars
from main import main

if __name__ == '__main__':
    main()
