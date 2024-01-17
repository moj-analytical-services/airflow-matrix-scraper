import pytest
import os


def pytest_configure(config):
    # Set the PYTHONPATH to be PWD
    pythonpath = "."
    os.environ["PYTHONPATH"] = pythonpath


def pytest_addoption(parser):
    parser.addoption("--scrape_date", action="store", default="2023-12-25")
    parser.addoption("--env", action="store", default="preprod")


@pytest.fixture
def scrape_date(request):
    return request.config.getoption("--scrape_date")


@pytest.fixture
def env(request):
    return request.configgit.getoption("--env")


def pytest_unconfigure(config):
    # Remove the PYTHONPATH environment variable
    if "PYTHONPATH" in os.environ:
        del os.environ["PYTHONPATH"]
