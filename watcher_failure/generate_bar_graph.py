import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Any


def generate_bar_graph(
    statistics: Dict[str, int],
    output_path: str
) -> str:
    """
    Generate a bar chart from `statistics` mapping reasons to counts,
    save it to `output_path`, and return a text reference to the image.

    Args:
        statistics: dict of {reason: count}
        output_path: file path to save the PNG chart

    Returns:
        A simple text snippet referencing the saved chart file.
    """
    # Ensure output directory exists
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Prepare data
    reasons = list(statistics.keys())
    counts = list(statistics.values())

    # Create bar chart
    plt.figure()
    plt.bar(range(len(counts)), counts)
    plt.xticks(range(len(reasons)), reasons, rotation=45, ha='right')
    plt.ylabel('Count')
    plt.title('Failure Counts')
    plt.tight_layout()

    # Save chart
    plt.savefig(str(out_path))
    plt.close()

    # Return reference text
    return f"Chart saved: {out_path.name}"
