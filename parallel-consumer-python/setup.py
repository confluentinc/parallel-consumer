#
# Copyright (C) 2020-2022 Confluent, Inc.
#

from pathlib import Path
from string import Template
from xml.etree.ElementTree import parse as parse_xml
from setuptools import setup, find_packages
import shutil

with open('requirements.txt', 'r', encoding='utf-8') as requirements_file:
    requirements = requirements_file.readlines()

version = '0.1.0a0'

project_root = Path(__file__).parent
repo_root = project_root.parent
python_package_path = project_root / 'pyallel_consumer'
pc_pom_path = repo_root / 'pom.xml'
pc_version = parse_xml(pc_pom_path).getroot().find('{http://maven.apache.org/POM/4.0.0}version').text
pom_template = Template(Path('./pom-template.xml').read_text())
pom = pom_template.substitute(package_version=version,
                              pc_version=pc_version)
python_package_path.joinpath('pom.xml').write_text(pom)

# Can't be a hidden dir with preceding . or setuptools won't find it
shutil.copytree(repo_root / '.mvn', python_package_path / 'mvn', dirs_exist_ok=True)
copy_file_into_package = lambda file_name: shutil.copy(repo_root / file_name, python_package_path / file_name)
shutil.copy(repo_root / 'mvnw', python_package_path)
shutil.copy(repo_root / 'mvnw.cmd', python_package_path)

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
