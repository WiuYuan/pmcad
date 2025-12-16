import gzip
from lxml import etree
from pprint import pprint

xml_path = "/data/wyuan/workspace/pmcdata_pro/data/interpro/interpro.xml.gz"

count = 0
context = etree.iterparse(gzip.open(xml_path), events=("end",), tag="interpro")

for _, elem in context:
    ipr_id = elem.attrib.get("id")
    sub_tags = [c.tag for c in elem]
    if any("class_list" in t or "parent_list" in t or "child_list" in t for t in sub_tags):
        print(f"\n=== Entry {ipr_id} ===")
        for child in elem:
            if any(x in child.tag for x in ["class_list", "parent_list", "child_list"]):
                print("Tag:", child.tag)
                print(etree.tostring(child, encoding="unicode")[:800])
        count += 1
        if count >= 3:
            break
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]