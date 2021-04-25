"""
Unit tests for the calculator library
"""
from main.calc import Calculator


class TestCalculator:
    def setup_method(self, method):
        self.calc = Calculator()

    def test_addition(self):
        assert 4 == self.calc.add(2, 2)

    def test_subtraction(self):
        assert 2 == self.calc.subtract(4, 2)
