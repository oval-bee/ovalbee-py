from typing import Dict, List


# Check the annotation format documentation at
def get_sly_meta_from_annotation(ann_json: dict):
    """Generate sly.ProjectMeta from JSON annotation file."""
    import supervisely as sly
    from supervisely.annotation.annotation import AnnotationJsonFields
    from supervisely.annotation.label import LabelJsonFields

    SLY_IMAGE_ANN_KEYS = [
        AnnotationJsonFields.LABELS,
        AnnotationJsonFields.IMG_TAGS,
        AnnotationJsonFields.IMG_SIZE,
    ]

    meta = sly.ProjectMeta()
    if "annotation" in ann_json:
        ann_json = ann_json.get("annotation", {})

    if not all(key in ann_json for key in SLY_IMAGE_ANN_KEYS):
        return meta

    ann_objects = ann_json.get(AnnotationJsonFields.LABELS, [])
    for object in ann_objects:
        obj_tags = object.get(LabelJsonFields.TAGS, None)
        if obj_tags is None:
            obj_tags = []
        meta = create_tags_from_annotation(obj_tags, meta)
        meta = create_classes_from_annotation(object, meta)
    img_tags = ann_json.get(AnnotationJsonFields.IMG_TAGS, None)
    if img_tags is None:
        img_tags = []
    meta = create_tags_from_annotation(img_tags, meta)
    return meta


def create_tags_from_annotation(tags: List[dict], meta):
    import supervisely as sly
    from supervisely.annotation.tag import TagJsonFields

    meta: sly.ProjectMeta
    for tag in tags:
        if not TagJsonFields.TAG_NAME in tag:
            continue
        tag_name = tag[TagJsonFields.TAG_NAME]
        tag_value = tag.get(TagJsonFields.VALUE)
        if tag_value is None:
            tag_meta = sly.TagMeta(tag_name, sly.TagValueType.NONE)
        elif isinstance(tag_value, int) or isinstance(tag_value, float):
            tag_meta = sly.TagMeta(tag_name, sly.TagValueType.ANY_NUMBER)
        else:
            tag_meta = sly.TagMeta(tag_name, sly.TagValueType.ANY_STRING)

        # check existing tag_meta in meta
        existing_tag = meta.get_tag_meta(tag_name)
        if existing_tag is None:
            meta = meta.add_tag_meta(tag_meta)
    return meta


def create_classes_from_annotation(object: dict, meta):
    import supervisely as sly
    from supervisely.annotation.json_geometries_map import GET_GEOMETRY_FROM_STR
    from supervisely.annotation.label import LabelJsonFields
    from supervisely.geometry.constants import LOC
    from supervisely.geometry.graph import NODES, KeypointsTemplate

    meta: sly.ProjectMeta
    SLY_OBJECT_KEYS = [
        LabelJsonFields.OBJ_CLASS_NAME,
        LabelJsonFields.TAGS,
        LabelJsonFields.GEOMETRY_TYPE,
    ]

    if not all(key in object for key in SLY_OBJECT_KEYS):
        return meta
    class_name = object[LabelJsonFields.OBJ_CLASS_NAME]
    geometry_type_str = object[LabelJsonFields.GEOMETRY_TYPE]
    obj_class = None

    try:
        geometry_type = GET_GEOMETRY_FROM_STR(geometry_type_str)
    except KeyError:
        return meta

    obj_class = None
    geometry_config = None
    if issubclass(geometry_type, sly.GraphNodes):
        if NODES in object:
            geometry_config = KeypointsTemplate()
            for uuid, node in object[NODES].items():
                if LOC in node and len(node[LOC]) == 2:
                    geometry_config.add_point(label=uuid, row=node[LOC][0], col=node[LOC][1])
    obj_class = sly.ObjClass(
        name=class_name, geometry_type=geometry_type, geometry_config=geometry_config
    )
    existing_class = meta.get_obj_class(class_name)

    if obj_class is None:
        return meta

    if existing_class is None:
        meta = meta.add_obj_class(obj_class)
    else:
        if existing_class.geometry_type != obj_class.geometry_type:
            obj_class = sly.ObjClass(name=class_name, geometry_type=sly.AnyGeometry)
            meta = meta.delete_obj_class(class_name)
            meta = meta.add_obj_class(obj_class)
    return meta
