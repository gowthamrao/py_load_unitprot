import gzip
import tracemalloc
from pathlib import Path

import pytest
from memory_profiler import memory_usage

from py_load_uniprot.transformer import transform_xml_to_tsv


def generate_large_xml_file(
    file_path: Path, num_entries: int, content_multiplier: int = 1
):
    """
    Generates a large, gzipped XML file with a specified number of UniProt entries.
    """
    entry_template = """<entry dataset="Swiss-Prot" created="2022-01-01" modified="2022-01-01" version="{v}">
  <accession>P{i}</accession>
  <name>TEST_{i}</name>
  <protein><recommendedName><fullName>Test protein {i}</fullName></recommendedName></protein>
  <organism><name type="scientific">Test organism</name><dbReference type="NCBI Taxonomy" id="99999"/></organism>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
  <comment type="function"><text>This is a test comment. {filler}</text></comment>
</entry>
"""
    # Use a simple filler to increase the size of each entry
    filler_content = "X" * content_multiplier
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?><uniprot xmlns="http://uniprot.org/uniprot">')
        for i in range(num_entries):
            f.write(
                entry_template.format(
                    i=i, v=i % 100, filler=filler_content * (i % 10)
                )
            )
        f.write("</uniprot>")


@pytest.mark.skip(
    reason="Memory profiling is slow and should be run manually for performance validation."
)
def test_transformer_memory_usage(tmp_path):
    """
    Tests that the memory usage of the XML transformer does not grow linearly
    with the number of entries, which would indicate a memory leak.
    """
    # 1. Generate two XML files: one baseline, one significantly larger
    small_xml_path = tmp_path / "small.xml.gz"
    large_xml_path = tmp_path / "large.xml.gz"
    output_dir_small = tmp_path / "output_small"
    output_dir_large = tmp_path / "output_large"

    # A small number of entries for the baseline measurement
    generate_large_xml_file(small_xml_path, num_entries=1000)
    # A much larger number of entries to detect memory leaks
    generate_large_xml_file(large_xml_path, num_entries=20000)

    # 2. Measure memory usage for the small file
    # We run the function via memory_usage, which samples memory consumption over time.
    mem_usage_small = memory_usage(
        (
            transform_xml_to_tsv,
            (small_xml_path, output_dir_small, "standard"),
            {"num_workers": 1},
        ),
        interval=0.1,  # Sample every 100ms
        timeout=120,  # Max 2 minutes for the test
    )
    max_mem_small = max(mem_usage_small)
    print(f"Max memory for small file (1k entries): {max_mem_small:.2f} MiB")

    # 3. Measure memory usage for the large file
    mem_usage_large = memory_usage(
        (
            transform_xml_to_tsv,
            (large_xml_path, output_dir_large, "standard"),
            {"num_workers": 1},
        ),
        interval=0.1,
        timeout=240,  # Allow more time for the larger file
    )
    max_mem_large = max(mem_usage_large)
    print(f"Max memory for large file (20k entries): {max_mem_large:.2f} MiB")

    # 4. Assert that the memory usage does not scale excessively
    # The memory for the large file should not be drastically more than for the
    # small one. A perfect stream parser would have O(1) memory, so the usage
    # should be very close. We allow for some overhead, but it should not be
    # proportional to the input size increase (20x).
    # A generous threshold: memory for the 20x larger file should be less than
    # 3x the memory for the smaller one. A memory leak would cause a much
    # larger increase.
    assert (
        max_mem_large < max_mem_small * 3
    ), "Memory usage increased disproportionately, suggesting a memory leak."


def test_transformer_tracemalloc(tmp_path):
    """
    Uses tracemalloc to assert that memory allocated within the transformer
    is properly released, focusing on the lxml iterparse loop.
    """
    xml_path = tmp_path / "test.xml.gz"
    output_dir = tmp_path / "output"
    # Generate a file large enough to show memory changes, but small enough to run quickly
    generate_large_xml_file(xml_path, num_entries=5000, content_multiplier=500)

    tracemalloc.start()

    # --- Initial Snapshot ---
    snap1 = tracemalloc.take_snapshot()

    # --- Run the function ---
    transform_xml_to_tsv(xml_path, output_dir, "standard", num_workers=1)

    # --- Final Snapshot ---
    snap2 = tracemalloc.take_snapshot()

    tracemalloc.stop()

    # --- Compare Snapshots ---
    top_stats = snap2.compare_to(snap1, "lineno")

    # It's normal for some objects to be created and stick around (e.g., file handles,
    # cached objects). The goal is to ensure there isn't a massive leak from the
    # XML parsing itself. We check the top memory-consuming lines.
    print("Top 10 memory differences after transformation:")
    for stat in top_stats[:10]:
        print(stat)

    # The most critical check: the total memory growth should be small.
    # We expect some growth due to caching, etc., but it should be minimal
    # compared to the size of the processed file.
    total_growth_kb = sum(stat.size for stat in top_stats) / 1024
    print(f"Total memory growth: {total_growth_kb:.2f} KB")

    # A very loose threshold: assert that the total memory growth is less than 1 MB.
    # A real leak from not clearing elements would be much larger. The generated
    # file is several megabytes, so if elements aren't cleared, the growth
    # would be on that order of magnitude.
    assert (
        total_growth_kb < 1024
    ), "Memory growth exceeded 1MB, suggesting objects were not properly garbage collected."
