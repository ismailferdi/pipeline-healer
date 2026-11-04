import pytest

from pipeline_healer import main


def test_main_prints_greeting(capsys: pytest.CaptureFixture[str]) -> None:
    main()

    assert "pipeline-healer" in capsys.readouterr().out
