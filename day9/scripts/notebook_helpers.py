"""Shared helpers — small markdown + code cells for Databricks SOURCE notebooks."""

from __future__ import annotations


def emit(parts: list[str], cell_type: str, content: str) -> None:
    if cell_type == "md":
        parts.append("# MAGIC %md")
        for line in content.strip().splitlines():
            parts.append(f"# MAGIC {line}" if line else "# MAGIC")
    else:
        parts.extend(content.strip().splitlines())
    parts.append("")
    parts.append("# COMMAND ----------")
    parts.append("")


def md(parts: list[str], text: str) -> None:
    emit(parts, "md", text)


def code(parts: list[str], text: str) -> None:
    emit(parts, "code", text)


def box_diagram(parts: list[str], diagram: str, caption: str = "Diagram") -> None:
    """ASCII box diagram — always visible in Databricks (no Mermaid)."""
    md(parts, f"#### {caption}\n\n```\n{diagram.strip()}\n```")


def ascii_flow(parts: list[str], diagram: str, caption: str = "Step-by-step flow") -> None:
    box_diagram(parts, diagram, caption)


def concept(
    parts: list[str],
    title: str,
    what: str,
    why: str,
    how: str,
    analogy: str,
    example_code: str,
    *,
    box_diagram_text: str | None = None,
    mermaid_diagram: str | None = None,
    ascii_diagram: str | None = None,
    extra_code: list[str] | None = None,
) -> None:
    """One concept → many small cells."""
    md(parts, f"### {title}")
    md(parts, f"**WHAT**\n\n{what}")
    md(parts, f"**WHY**\n\n{why}")
    diagram = box_diagram_text or mermaid_diagram or ascii_diagram
    if diagram:
        box_diagram(parts, diagram)
    md(parts, f"**HOW**\n\n{how}")
    md(parts, f"**ANALOGY**\n\n{analogy}")
    code(parts, example_code)
    if extra_code:
        for c in extra_code:
            code(parts, c)


def lesson(
    parts: list[str],
    title: str,
    theory: str,
    example_code: str,
    *,
    box_diagram_text: str | None = None,
    mermaid_diagram: str | None = None,
    ascii_diagram: str | None = None,
    extra_md: list[str] | None = None,
    extra_code: list[str] | None = None,
) -> None:
    md(parts, f"### {title}")
    for para in theory.strip().split("\n\n"):
        block = para.strip()
        if block:
            md(parts, block)
    diagram = box_diagram_text or mermaid_diagram or ascii_diagram
    if diagram:
        box_diagram(parts, diagram)
    if extra_md:
        for block in extra_md:
            md(parts, block)
    code(parts, example_code)
    if extra_code:
        for c in extra_code:
            code(parts, c)


def finish(parts: list[str]) -> list[str]:
    while parts and parts[-1] == "# COMMAND ----------":
        parts.pop()
    if parts and parts[-1] == "":
        parts.pop()
    return parts
