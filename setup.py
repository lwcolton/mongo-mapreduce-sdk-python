import os.path
from setuptools import setup

package_dir = os.path.abspath(os.path.dirname(__file__))
version_file = os.path.join(package_dir, "version")
with open(version_file) as version_file_handle:
    version = version_file_handle.read()

setup(
    name="mreduce",
    install_requires=["pymongo"],
    version=version,
    author="MReduce",
    author_email="support@mreduce.com",
    description="MReduce python sdk",
    url="https://mreduce.com",
    packages=["mreduce"],
    classifiers=[],
    python_requires='>=2.7',
)
