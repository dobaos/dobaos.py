import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dobaos",
    version="0.0.2",
    author="Vladimir Shabunin",
    author_email="va.shabunin@physics.msu.ru",
    description="Python client for dobaos service.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dobaos/dobaos.py",
    packages=setuptools.find_packages(),
    install_requires=[
      'redis',
    ],
    keywords=['KNX', 'BAOS', 'dobaos', 'bobaos'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*",
)

