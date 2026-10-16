"""BankFlow Sentinel - Batch ID and Path Conversion Utilities.

Converts between database batch IDs ("B001") and directory/file names ("batch_001")
according to docs/contracts.md specifications.
"""

import re
from typing import Tuple, Union


def batch_num_to_batch_id(batch_num: int) -> str:
    """Convert an integer batch number to a database batch ID.

    Example:
        1 -> "B001"
        42 -> "B042"
    """
    return f"B{batch_num:03d}"


def batch_id_to_dir_name(batch_id: str) -> str:
    """Convert a database batch ID to directory/file prefix.

    Example:
        "B001" -> "batch_001"
        "b001" -> "batch_001"
    """
    match = re.match(r"^[Bb](\d+)$", batch_id.strip())
    if match:
        num = int(match.group(1))
        return f"batch_{num:03d}"
    raise ValueError(f"Invalid batch_id format: '{batch_id}'. Expected 'B001' or similar.")


def dir_name_to_batch_id(dir_name: str) -> str:
    """Convert directory/file name to database batch ID.

    Example:
        "batch_001" -> "B001"
    """
    match = re.match(r"^batch_(\d+)$", dir_name.strip())
    if match:
        num = int(match.group(1))
        return f"B{num:03d}"
    raise ValueError(f"Invalid dir_name format: '{dir_name}'. Expected 'batch_001' or similar.")


def normalize_batch(batch_val: Union[int, str]) -> Tuple[str, str]:
    """Normalize any batch representation into (batch_id, dir_name) tuple.

    Examples:
        1 -> ("B001", "batch_001")
        "B001" -> ("B001", "batch_001")
        "batch_001" -> ("B001", "batch_001")
    """
    if isinstance(batch_val, int):
        b_id = batch_num_to_batch_id(batch_val)
        return b_id, batch_id_to_dir_name(b_id)

    val = str(batch_val).strip()
    if val.isdigit():
        b_id = batch_num_to_batch_id(int(val))
        return b_id, batch_id_to_dir_name(b_id)

    if re.match(r"^[Bb]\d+$", val):
        b_id = f"B{int(val[1:]):03d}"
        return b_id, batch_id_to_dir_name(b_id)

    if re.match(r"^batch_\d+$", val):
        b_id = dir_name_to_batch_id(val)
        return b_id, val

    raise ValueError(f"Cannot normalize batch value: '{batch_val}'.")
