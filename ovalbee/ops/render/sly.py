from typing import Optional, Tuple

from ovalbee.ops.convert.sly import get_sly_meta_from_annotation

BBOX_THICKNESS_PERCENT = 0.3  # percent of the smaller image side
# OUTPUT_WIDTH_PX
MASK_OPACITY = 0.7
BBOX_OPACITY = 1
FILLBBOX_OPACITY = 0.4
HEATMAP_THRESHOLD = 0.2


def visualize_sly(
    img,  # : np.ndarray,
    annotation_file: str,
    metadata: Optional[dict] = None,
):  # -> np.ndarray:
    """Visualize SLY annotation on the given image"""
    import cv2
    import numpy as np
    from PIL import Image

    import supervisely as sly

    img: np.ndarray

    def get_thickness(render: np.ndarray, thickness_percent: float, from_min=False) -> int:
        render_height, render_width, _ = render.shape
        render_side = render_width
        if from_min:
            render_side = min(render_height, render_width)
        return int(render_side * thickness_percent / 100)

    def color_map(
        img_size, data: np.ndarray, origin: sly.PointLocation, threshold: float
    ) -> np.ndarray:
        mask = np.zeros(img_size, dtype=np.uint8)
        x, y = origin.col, origin.row
        h, w = data.shape[:2]
        threshold = threshold or 0.2
        threshold = int(255 * threshold)
        mask[y : y + h, x : x + w] = data
        mask[mask < threshold] = 0
        cv2.normalize(mask, mask, 0, 255, cv2.NORM_MINMAX)
        mask = cv2.applyColorMap(mask, cv2.COLORMAP_JET)
        BG_COLOR = np.array([128, 0, 0], dtype=np.uint8)
        mask = np.where(mask == BG_COLOR, 0, mask)
        return mask

    ann_dict = sly.json.load_json_file(annotation_file)
    sly_meta = metadata.get("sly_meta") if metadata else None
    if sly_meta is not None:
        sly_meta = sly.ProjectMeta.from_json(sly_meta)
    else:
        sly_meta = get_sly_meta_from_annotation(ann_dict)

    ann = sly.Annotation.from_json(ann_dict, sly_meta)

    render_mask, render_bbox, render_fillbbox = (
        np.zeros((ann.img_size[0], ann.img_size[1], 3), dtype=np.uint8),
        np.zeros((ann.img_size[0], ann.img_size[1], 3), dtype=np.uint8),
        np.zeros((ann.img_size[0], ann.img_size[1], 3), dtype=np.uint8),
    )

    alpha_masks = []
    for label in ann.labels:
        label: sly.Label
        if type(label.geometry) == sly.Point:
            label.draw(
                render_mask,
                thickness=get_thickness(render_mask, thickness_percent=3, from_min=True),
            )
        elif type(label.geometry) in (sly.GraphNodes, sly.Polyline):
            label.draw(
                render_mask,
                thickness=get_thickness(render_mask, thickness_percent=2, from_min=True),
            )
        elif type(label.geometry) in (sly.Cuboid2d,):
            label.draw(
                render_mask,
                thickness=get_thickness(render_mask, thickness_percent=1, from_min=True),
            )
        elif type(label.geometry) == sly.Rectangle:
            thickness = get_thickness(render_bbox, BBOX_THICKNESS_PERCENT)
            label.draw_contour(
                render_bbox,
                thickness=thickness,
                # draw_tags=draw_tags, #TODO fix (0,0,0,255) color font
                # draw_class_name=draw_class_names,
            )
            label.draw(render_fillbbox)
        elif type(label.geometry) == sly.AlphaMask:
            alpha_masks.append(label)
        else:
            label.draw(
                render_mask,
                # draw_tags=draw_tags,
                # draw_class_name=draw_class_names,
            )
    if len(alpha_masks) > 0:
        temp_mask = render_mask.copy()
        for label in alpha_masks:
            temp = color_map(
                ann.img_size, label.geometry.data, label.geometry.origin, HEATMAP_THRESHOLD
            )
            temp_mask = np.where(np.any(temp > 0, axis=-1, keepdims=True), temp, temp_mask)
            # render_mask = np.where(np.any(temp > 0, axis=-1, keepdims=True), temp, render_mask)
        # render_mask = cv2.addWeighted(render_mask, 0.5, temp_mask, 0.5, 0)
        temp_mask = cv2.cvtColor(temp_mask, cv2.COLOR_BGR2RGB)
        render_mask = np.where(
            np.any(temp_mask > 0, axis=-1, keepdims=True), temp_mask, render_mask
        )

    alpha_mask = (MASK_OPACITY - np.all(render_mask == [0, 0, 0], axis=-1).astype("uint8")) * 255
    alpha_mask[alpha_mask < 0] = 0

    alpha_bbox = (BBOX_OPACITY - np.all(render_bbox == [0, 0, 0], axis=-1).astype("uint8")) * 255
    alpha_bbox[alpha_bbox < 0] = 0

    alpha_fillbbox = (
        FILLBBOX_OPACITY - np.all(render_fillbbox == [0, 0, 0], axis=-1).astype("uint8")
    ) * 255
    alpha_fillbbox[alpha_fillbbox < 0] = 0

    alpha = np.where(alpha_mask != 0, alpha_mask, alpha_fillbbox)
    alpha = np.where(alpha_bbox != 0, alpha_bbox, alpha)

    rgb = np.where(render_mask != 0, render_mask, render_fillbbox)
    rgb = np.where(render_bbox != 0, render_bbox, rgb)

    rgba = np.dstack((rgb, alpha))

    img_pil = Image.fromarray(img.astype("uint8")).convert("RGBA")
    render_pil = Image.fromarray(rgba.astype("uint8")).convert("RGBA")
    combined = Image.alpha_composite(img_pil, render_pil)
    img = np.array(combined.convert("RGB"))

    return img
