import asyncio
import runpy


def test_all_mcp_tools_registered_on_canonical_instance() -> None:
    """Regression guard for the Sprint 13.5 dual-FastMCP-instance bug fixed in this MICRO PR."""
    from src.mcp.server import mcp

    tools = asyncio.run(mcp.list_tools())
    tool_names = {tool.name for tool in tools}

    assert tool_names == {
        "healthcheck",
        "validate_task_spec",
        "suggest_next_pr_number",
        "get_task_schema",
        "get_agents_md_template",
        "get_repo_task_status",
    }


def test_main_module_import_safe_under_pytest() -> None:
    import src.mcp.__main__ as main_module
    import src.mcp.server as server

    assert main_module.main is server.main
    calls = []
    original_main = server.main
    try:
        server.main = lambda: calls.append("main")
        runpy.run_path("src/mcp/__main__.py", run_name="__main__")
    finally:
        server.main = original_main
    assert calls == ["main"]
