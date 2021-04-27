"""
This test file will not check all the scenarios present in config file 
because of mechanical tasks. Most of those are very similar. Can be done 
in future if required.
"""

from sys import modules
import pytest

from main.utils.config_read import ConfigReader, ReturnStatus

class TestConfigReader:
    def test_non_existent_file_read(self, capsys):
        cfg_reader = ConfigReader('tests/config/config/non_existent.json')
        out, err = capsys.readouterr()
        assert "FileNotFoundError" in err
        assert ReturnStatus.FILE_NOT_FOUND == cfg_reader.file_read_status()

    def test_invalid_file_read(self, capsys):
        cfg_reader = ConfigReader('tests/config/config.invalid.json')
        out, err = capsys.readouterr()
        assert "Exception Decoding" in err
        assert ReturnStatus.DECODE_FAIL == cfg_reader.file_read_status()

    def test_missing_entry_read(self, capsys):
        cfg_reader = ConfigReader('tests/config/config.missing.entry.json')
        out, err = capsys.readouterr()
        assert "Exception thrown" in err
        assert ReturnStatus.FAILURE == cfg_reader.file_read_status()

    def test_log_level(self, cfg_read):
        assert cfg_read.monitoring_interval == 10

    def test_invalid_log_set(self, cfg_read):
        with pytest.raises(ValueError):
            cfg_read.log_level = 7878


    def test_valid_log_set(self, cfg_read):
        cfg_read.log_level = 20
        assert cfg_read.log_level == 20
