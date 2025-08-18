import os
import xml.etree.ElementTree as ET
import xml.dom.minidom


def get_environment_variables():
    username = os.getenv("CENTRAL_TOKEN_USERNAME")
    password = os.getenv("CENTRAL_TOKEN_PASSWORD")
    server_id = os.getenv("SONATYPE_SERVER_ID")
    settings_xml_path = os.getenv("SETTINGS_XML_PATH")

    if not (username and password and server_id and settings_xml_path):
        print("Error: One or more environment variables are not set.")
        exit(1)

    return username, password, server_id, settings_xml_path


def find_or_create_servers_element(root):
    namespace_uri = "http://maven.apache.org/SETTINGS/1.0.0"
    ET.register_namespace("", namespace_uri)
    namespace = {'maven': namespace_uri}
    servers = root.find(".//maven:servers", namespaces=namespace)
    if servers is None:
        servers = ET.SubElement(root, "servers")
    return servers


def check_server_existence(servers, server_id):
    namespace = {'maven': 'http://maven.apache.org/SETTINGS/1.0.0'}
    existing_server_ids = [server.find("maven:id", namespaces=namespace).text for server in
                           servers.findall("maven:server", namespaces=namespace)]
    return server_id in existing_server_ids


def add_server_element(servers, server_id, username, password):
    new_server = ET.SubElement(servers, "server")
    id_elem = ET.SubElement(new_server, "id")
    id_elem.text = server_id
    username_elem = ET.SubElement(new_server, "username")
    username_elem.text = username
    password_elem = ET.SubElement(new_server, "password")
    password_elem.text = password
    print(f"Added server with ID '{server_id}' to settings.xml.")


def write_settings_xml(tree, settings_xml_path):
    tree_str = ET.tostring(tree.getroot(), encoding="utf-8", method="xml")
    xml_str = xml.dom.minidom.parseString(tree_str)
    with open(settings_xml_path, "w", encoding="utf-8") as f:
        f.write(xml_str.toprettyxml(newl=""))


def main():
    username, password, server_id, settings_xml_path = get_environment_variables()
    tree = ET.parse(settings_xml_path)
    servers = find_or_create_servers_element(tree.getroot())
    if check_server_existence(servers, server_id):
        print(f"Server with ID '{server_id}' already exists. No updates are made")
    else:
        add_server_element(servers, server_id, username, password)
        write_settings_xml(tree, settings_xml_path)
        print("Updated settings.xml with server information.")


if __name__ == "__main__":
    main()
