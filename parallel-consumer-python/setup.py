#
# Copyright (C) 2020-2022 Confluent, Inc.
#

import logging
import re
import shutil
import subprocess
from pathlib import Path
from string import Template
from xml.etree.ElementTree import parse as parse_xml

from setuptools import setup, find_packages


def run_command(*commands: str):
    process = subprocess.run([*commands], capture_output=True)
    if process.returncode != 0:
        raise EnvironmentError(f'Return code {process.returncode} '
                               f'{process.stderr.decode()} {process.stdout.decode()}')
    logging.debug(process)
    return process.stdout.decode()


version = '0.1.0a0'

project_root = Path(__file__).parent
repo_root = project_root.parent
mvnw_path = repo_root / 'mvnw'

requirements = [requirement.strip() for requirement in (project_root / 'requirements.txt').read_text().splitlines()
                if not requirement.startswith('#')]

package_name = 'pyallel_consumer'
python_package_path = project_root / package_name
pc_pom_path = repo_root / 'pom.xml'
pc_version = parse_xml(pc_pom_path).getroot().find('{http://maven.apache.org/POM/4.0.0}version').text
pom_template = Template(Path('./pom-template.xml').read_text())
pom = pom_template.substitute(package_version=version,
                              pc_version=pc_version)
python_package_path.joinpath('pom.xml').write_text(pom)

# Can't be a hidden dir with preceding . or setuptools won't find it
shutil.copytree(repo_root / '.mvn', python_package_path / 'mvn', dirs_exist_ok=True)
copy_file_into_package = lambda file_name: shutil.copy(repo_root / file_name, python_package_path / file_name)
shutil.copy(mvnw_path, python_package_path)
shutil.copy(repo_root / 'mvnw.cmd', python_package_path)

pc_core_dir = repo_root / 'parallel-consumer-core'
run_command(str(mvnw_path), 'clean', 'package', '-DskipTests', '-f', str(pc_core_dir))
jar_regex = 'parallel-consumer-core-[0-9].[0-9].[0-9].?[0-9]?(-SNAPSHOT)?-jar-with-dependencies.jar'
pc_jar_path = next((path for path in (pc_core_dir / 'target').glob('*') if re.search(jar_regex, str(path))))
bundled_jar_path = python_package_path / pc_jar_path.name
bundled_jar_path.write_bytes(pc_jar_path.read_bytes())

setup(
    name=package_name,
    version=version,
    description="Python wrapper for Confluent Parallel Consumer",
    packages=find_packages(exclude=['*tests*']),
    install_requires=requirements,
    package_data={'': ['**mvn/**', 'mvnw', 'mvnw.cmd', 'pom.xml', '*.jar']},
    long_description='Python wrapper for '
                     '[Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer)',
    long_description_content_type='text/markdown',
    license='Apache License Version 2.0'
)
