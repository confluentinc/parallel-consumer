from xml.etree.ElementTree import parse as parse_xml

from setuptools import setup, find_packages

with open('requirements.txt', 'r', encoding='utf-8') as requirements_file:
    requirements = requirements_file.readlines()

pom = parse_xml('./pyallel_consumer/pom.xml').getroot()
version = pom.find('{http://maven.apache.org/POM/4.0.0}version').text
setup(
    name='pyallel_consumer',
    version=version,
    description="Python wrapper for Confluent Parallel Consumer",
    packages=find_packages(),
    install_requires=requirements,
    package_data={'': ['**mvn/**', 'mvnw', 'mvnw.cmd', 'pom.xml']},
    long_description='Python wrapper for '
                     '[Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer)',
    long_description_content_type='text/markdown',
    license='Apache License Version 2.0'
)
