#
# Copyright (C) 2020-2022 Confluent, Inc.
#

import logging
import subprocess
import sys
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from xml.etree.ElementTree import parse as parse_xml, Element

import jdk
import jpype.imports
from jpype._jvmfinder import LinuxJVMFinder, DarwinJVMFinder, WindowsJVMFinder, JVMNotFoundException


def load_config(config_path: str) -> 'Properties':
    from java.io import FileInputStream
    from java.util import Properties

    config_path = Path(config_path).expanduser()
    if not config_path.exists():
        raise IOError(f'{config_path} not found')
    config = Properties()
    with FileInputStream(config_path) as input_stream:
        config.load(input_stream)
    return config


def _find_jre(java_home: str) -> str:
    if sys.platform == "win32":
        finder = WindowsJVMFinder()
    elif sys.platform == "darwin":
        finder = DarwinJVMFinder()
    else:
        finder = LinuxJVMFinder()
    return finder.find_libjvm(java_home)


@wraps(jdk.install)
def _install_jre(version: str = '17', *args, **kwargs) -> str:
    print('Installing JRE')
    try:
        jre_install_dir = jdk.install(version, *args, jre=True, **kwargs)
    except PermissionError as e:
        print(str(e))
        jre_install_dir = Path(e.filename.split('/Contents/')[0])
        print(f'Assuming install dir is {jre_install_dir}')
    jvm_path = _find_jre(jre_install_dir)
    assert Path(jvm_path).exists(), f'{jvm_path} does not exist'
    return jvm_path


class _Pom:
    @dataclass
    class Dependency:
        group_id: str
        artifact_id: str
        version: str

        @classmethod
        def parse(cls, element: Element, tag_prefix: str) -> 'Dependency':
            get_child_text = lambda parent, child_name: parent.find(f'{tag_prefix}{child_name}').text
            return cls(get_child_text(element, 'groupId'),
                       get_child_text(element, 'artifactId'),
                       get_child_text(element, 'version'))

    def __init__(self, path: str):
        self._path = path
        self._element_tree = None
        self._tag_prefix = '{http://maven.apache.org/POM/4.0.0}'

    @property
    def element_tree(self):
        if not self._element_tree:
            self._element_tree = parse_xml(self._path)
        return self._element_tree

    def get_dependency(self, artifact_id: str) -> Dependency:
        root = self.element_tree.getroot()
        for dependency in root.find(f'{self._tag_prefix}dependencies').findall(f'{self._tag_prefix}dependency'):
            if dependency.find(f'{self._tag_prefix}artifactId').text == 'parallel-consumer-core':
                return self.Dependency.parse(dependency, self._tag_prefix)
        raise ValueError(f'{artifact_id} not found in dependencies')


class _Maven:
    def __init__(self):
        self._project_dir = Path(__file__).parent
        self._mvn_path = self._project_dir / 'mvnw'
        assert self._mvn_path.exists()
        self._local_repo_path = self._project_dir / 'local-maven-repo'

    def install_dependencies(self):
        logging.info('Installing Maven dependencies')
        self._install_jar_to_local_repo('parallel-consumer-core')
        self._run_command('install')

    def get_class_path(self):
        return self._run_command('-q', 'exec:exec', '-Dexec.executable=echo', '-Dexec.args="%classpath"')

    def _install_jar_to_local_repo(self, artifact_id: str):
        pom = _Pom(self._project_dir / 'pom.xml')
        dependency = pom.get_dependency(artifact_id)
        jar_path = self._project_dir / f'{dependency.artifact_id}-{dependency.version}-jar-with-dependencies.jar'
        self._run_command('deploy:deploy-file',
                          f'-DgroupId={dependency.group_id}',
                          f'-DartifactId={dependency.artifact_id}',
                          f'-Dversion={dependency.version}',
                          f'-Durl=file:{str(self._local_repo_path)}',
                          f'-DrepositoryId={self._local_repo_path.name}',
                          '-DupdateReleaseInfo=true',
                          f'-Dfile={str(jar_path)}')

    def _run_command(self, *commands: str):
        process = subprocess.run([str(self._mvn_path), *commands, '-f', str(self._project_dir)], capture_output=True)
        if process.returncode != 0:
            raise EnvironmentError(f'Return code {process.returncode} '
                                   f'{process.stderr.decode()} {process.stdout.decode()}')
        logging.debug(process)
        return process.stdout.decode()


def start_jvm():
    if jpype.isJVMStarted():
        return
    try:
        jvm_path = _find_jre(jdk._JRE_DIR)
        logging.debug(f'JRE already installed at {jdk._JRE_DIR}')
    except JVMNotFoundException:
        jvm_path = _install_jre()
    maven = _Maven()
    maven.install_dependencies()
    class_path = maven.get_class_path()
    logging.info('Starting JVM')
    jpype.startJVM(jvmpath=jvm_path, classpath=[class_path])


jpype.imports.registerDomain("jio", alias="io")
start_jvm()
