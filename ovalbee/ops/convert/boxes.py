try:
    import numpy as np
except ImportError:
    pass


def xywh_to_xyxy(xywh: np.ndarray) -> np.ndarray:
    xyxy = xywh.copy()
    xyxy[:, 2] = xywh[:, 0] + xywh[:, 2]
    xyxy[:, 3] = xywh[:, 1] + xywh[:, 3]
    return xyxy


def xyxy_to_xywh(xyxy: np.ndarray) -> np.ndarray:
    xywh = xyxy.copy()
    xywh[:, 2] = xyxy[:, 2] - xyxy[:, 0]
    xywh[:, 3] = xyxy[:, 3] - xyxy[:, 1]
    return xywh


def xcycwh_to_xyxy(xcycwh: np.ndarray) -> np.ndarray:
    xyxy = xcycwh.copy()
    xyxy[:, 0] = xcycwh[:, 0] - xcycwh[:, 2] / 2
    xyxy[:, 1] = xcycwh[:, 1] - xcycwh[:, 3] / 2
    xyxy[:, 2] = xcycwh[:, 0] + xcycwh[:, 2] / 2
    xyxy[:, 3] = xcycwh[:, 1] + xcycwh[:, 3] / 2
    return xyxy


def xyxy_to_xcycwh(xyxy: np.ndarray) -> np.ndarray:
    xcycwh = xyxy.copy()
    xcycwh[:, 0] = (xyxy[:, 0] + xyxy[:, 2]) / 2
    xcycwh[:, 1] = (xyxy[:, 1] + xyxy[:, 3]) / 2
    xcycwh[:, 2] = xyxy[:, 2] - xyxy[:, 0]
    xcycwh[:, 3] = xyxy[:, 3] - xyxy[:, 1]
    return xcycwh


def normalize_boxes_xyxy(boxes: np.ndarray, img_width: int, img_height: int) -> np.ndarray:
    norm_boxes = boxes.copy()
    norm_boxes[:, 0] /= img_width
    norm_boxes[:, 1] /= img_height
    norm_boxes[:, 2] /= img_width
    norm_boxes[:, 3] /= img_height
    return norm_boxes


def denormalize_boxes_xyxy(norm_boxes: np.ndarray, img_width: int, img_height: int) -> np.ndarray:
    boxes = norm_boxes.copy()
    boxes[:, 0] *= img_width
    boxes[:, 1] *= img_height
    boxes[:, 2] *= img_width
    boxes[:, 3] *= img_height
    return boxes

