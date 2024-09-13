from setuptools import find_packages, setup

setup(
    name="pokemon_pipeline",
    packages=find_packages(exclude=["pokemon_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
