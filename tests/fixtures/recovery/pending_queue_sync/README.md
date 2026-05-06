This fixture records the queue-sync recovery shape. A queue-done PR can be open
while ordinary task headers still parse normally, and later recovery work must
preserve both facts. The helper projection focuses on the current queue; PR-266b
will assert the pending queue-sync branch in the full recovery path.
