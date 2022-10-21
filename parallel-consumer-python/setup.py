#
# Copyright (C) 2020-2022 Confluent, Inc.
#

from pathlib import Path
from string import Template
from xml.etree.ElementTree import parse as parse_xml
from setuptools import setup, find_packages

with open('requirements.txt', 'r', encoding='utf-8') as requirements_file:
    requirements = requirements_file.readlines()

version = '0.1.0a0'

pc_pom_path = Path(__file__).parent.parent / 'pom.xml'
pc_version = parse_xml(pc_pom_path).getroot().find('{http://maven.apache.org/POM/4.0.0}version').text
pom_template = Template(Path('./pom-template.xml').read_text())
pom = pom_template.substitute(package_version=version,
                              pc_version=pc_version)
Path('./pyallel_consumer/pom.xml').write_text(pom)

setup(
    name='pyallel_consumer',
    version=version,
    description="Python wrapper for Confluent Parallel Consumer",
    packages=find_packages(exclude=['*tests*']),
    install_requires=requirements,
    package_data={'': ['**mvn/**', 'mvnw', 'mvnw.cmd', 'pom.xml']},
    long_description='Python wrapper for '
                     '[Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer)',
    long_description_content_type='text/markdown',
    license='Apache License Version 2.0'
)
