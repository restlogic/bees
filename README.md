# bees
**This project is a library for distribution system security tracing.**

## Build 

python3 setup.py sdist

then you shall find `dist/bees-0.0.1.tar.gz`

## Use pypiserver

1. pip install pypiserver

2. put bees-0.0.1.tar.gz in ./packages

3. pypi-server -p 8080 ./packages 

4. Browser visit http://:8080/
