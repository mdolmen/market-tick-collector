"""Venue adapters. An adapter's only job is one venue's frames → the model.

Everything venue-shaped lives behind this boundary: the frame dialect, the
bootstrap, and the sequencing rule. Nothing downstream — book, sink, metrics —
may learn which venue a record came from except as a label. If it needs to, the
boundary is in the wrong place.
"""
