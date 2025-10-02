from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
import pybind11
import os

# 定义C++扩展
ext_modules = [
    Extension(
        "pmcad._core",
        [
            "src/cpp/bindings.cpp",
            "src/cpp/reader.cpp",
            "src/cpp/gene_match.cpp",
        ],
        include_dirs=[
            "src/cpp",
            pybind11.get_include(),
        ],
        language="c++",
        extra_compile_args=["-std=c++17", "-O3", "-fPIC"],
    ),
]

setup(
    name="pmcad",
    version="0.1.0",
    author="Your Name",
    description="A C++ accelerated TSV file reader",
    packages=["pmcad"],
    package_dir={"": "src"},
    ext_modules=ext_modules,
    zip_safe=False,
    python_requires=">=3.6",
    install_requires=["pybind11>=2.6.0", "pandas>=1.0.0"],  # 可选，用于DataFrame功能
)
