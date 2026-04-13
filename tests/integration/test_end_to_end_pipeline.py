from __future__ import annotations

from src.generators.source_a_generator import generate_source_a
from src.generators.source_b_generator import generate_source_b


def test_generators_create_seed_files(tmp_path):
    generate_source_a(output_dir=str(tmp_path / "source_a"))
    generate_source_b(output_dir=str(tmp_path / "source_b"))

    assert (tmp_path / "source_a" / "customers_full.csv").exists()
    assert (tmp_path / "source_b" / "customers_full.csv").exists()
