# test_with_unittest.py
from unittest import TestCase
from xml.etree.ElementInclude import include
import a


class TryTesting(TestCase):
    
    def test_always_passes(self):
        assert a.test_sum(3,4) == 7
     
    def test_2(self):
        assert a.test_sum(2,4) == 6    

    