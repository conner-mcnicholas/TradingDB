import setuptools
from distutils.core import setup

setup(
    name='pipeline_orchestration',
    version='0.0.1',
    description='Data pipeline for ingesting and analyzing csv+json trading data',
    author='Conner McNicholas',
    author_email='connermcnicholas@gmail.com.com',
    url='',
    python_requires=">=3.7",
    packages=setuptools.find_packages(),
    package_dir={
        "pipeline_orchestrator": "pipeline_orchestrator",
        "pipeline_orchestrator.analysis": "pipeline_orchestrator/analysis",
        "pipeline_orchestrator.ingestion": "pipeline_orchestrator/ingestion",
        "pipeline_orchestrator.preprocess": "pipeline_orchestrator/preprocess",
        "pipeline_orchestrator.tracker": "pipeline_orchestrator/tracker",
    }
)
