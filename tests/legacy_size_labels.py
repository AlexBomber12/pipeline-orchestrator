"""Fixture names for labels emitted by daemon versions before size migration."""


def continued_size_label(head_sha: str) -> str:
    return f"size-ok:{head_sha}"


def fixing_size_label(head_sha: str) -> str:
    return f"size-fix:{head_sha}"
