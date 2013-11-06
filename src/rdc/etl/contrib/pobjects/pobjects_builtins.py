"""Convenience module for method restrictions (@abstract, @final) etc.

Usage: import pobjects_builtins

You import once, use everywhere.
"""
import pobjects

# Updates for all modules.
for name in ('final', 'finalim', 'nosuper', 'abstract', 'override'):
  __builtins__[name] = getattr(pobjects, name)
