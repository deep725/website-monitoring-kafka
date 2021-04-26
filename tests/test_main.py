def test_mocking_classes(mocker):
    sq = mocker.MagicMock()
    sq.calculate_area.return_value = 1
    assert sq.calculate_area() == 1
