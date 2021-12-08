from setuptools import find_packages, setup

setup(
    name="sample-pipeline",
    version="1.0",
    author="Marc Wouts",
    author_email="marc.wouts@gmail.com",
    description="A sample Python pipeline",
    url="https://github.com/CFMTech/python_pipeline_blog_post",
    packages=find_packages(exclude=["tests"]),
    tests_require=["pytest"],
    install_requires=["pandas", "dask"],
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
