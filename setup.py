"""
setup.py — Packaging della libreria logistica_utils.

Installazione locale:
    pip install -e lib/

Installazione da wheel (Databricks cluster init script):
    pip install logistica_utils-1.0.0-py3-none-any.whl

Build wheel:
    cd lib && python setup.py bdist_wheel
"""

from setuptools import find_packages, setup

setup(
    name="logistica_utils",
    version="1.0.0",
    description="Libreria condivisa per il progetto Logistico 2.0 (Oracle → Databricks Medallion)",
    author="Team Logistico 2.0",
    author_email="luigi.scrimitore@aperion.it",
    python_requires=">=3.10",
    packages=find_packages(),
    # Runtime Spark/Delta sono FORNITI dall'ambiente Databricks (serverless): NON dichiararli
    # come dipendenze del wheel, altrimenti il serverless prova a reinstallarli (build lento
    # + "notebook command received after detach"). Restano in extras per l'uso locale/test.
    install_requires=[],
    extras_require={
        "dev": [
            "pyspark>=3.5",
            "delta-spark>=3.0",
            "pytest>=8.0",
            "pytest-cov>=5.0",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
