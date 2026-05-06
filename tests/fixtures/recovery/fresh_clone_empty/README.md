This fixture represents a fresh clone or bootstrap repository before any task
files are present. Header parsing should return no snapshot rather than raising
or fabricating queue entries. PR-266b will use the same fixture to verify the
full recovery path settles in IDLE with an empty queue.
