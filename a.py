import pytest


def test_sum(x,y):
    return x+y

def unit_test():
    assert test_sum(3,4) == 7
    
if __name__ == "__main__":
    test_sum(3,5)
    print("Everything passed")