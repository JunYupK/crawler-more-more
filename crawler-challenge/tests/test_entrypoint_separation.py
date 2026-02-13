"""mac/desktop 진입점 분리 검증 테스트.

실행 없이 소스 수준에서 import 경로를 검증하여,
mac과 desktop 기능이 섞이지 않도록 회귀를 방지한다.
"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent


def _read(rel_path: str) -> str:
    return (PROJECT_ROOT / rel_path).read_text(encoding="utf-8")


def test_mac_entrypoint_targets_ingestor_only():
    src = _read("mac/run.py")

    assert "from runners.ingestor_runner import main" in src
    assert "url_queue_runner" not in src
    assert "embedding_runner" not in src


def test_desktop_url_queue_entrypoint_targets_url_queue_runner():
    src = _read("desktop/run_url_queue.py")

    assert "from runners.url_queue_runner import main" in src
    assert "ingestor_runner" not in src


def test_desktop_embedding_entrypoint_targets_embedding_runner():
    src = _read("desktop/run_embedding.py")

    assert "from runners.embedding_runner import main" in src
    assert "ingestor_runner" not in src
