import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bees",
    version="0.0.4",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        'opentracing',
        'jaeger_client',
        'eventlet',
        'pyaml',
        'keystoneauth1',
        'python-keystoneclient',
        'sqlalchemy',
        'neutron_lib',
        'oslo_messaging',
        'oslo_service',
        'six',
        'webob'
        ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Natural Language :: Chinese (Simplified)",
    ],
    package_dir={"bees": "bees"},
    packages=setuptools.find_packages(),
)
