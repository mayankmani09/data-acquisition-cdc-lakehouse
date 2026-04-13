from __future__ import annotations

from src.generators.source_a_generator import generate_source_a
from src.generators.source_b_generator import generate_source_b
from src.generators.cdc_event_generator import generate_cdc_batch


def main() -> None:
    generate_source_a()
    generate_source_b()
    generate_cdc_batch(batch_id="batch_001")


if __name__ == "__main__":
    main()
