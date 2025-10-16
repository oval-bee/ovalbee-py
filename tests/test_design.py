# import ovalbee
# some logic to get API token and server address
# ob = ovalbee.Ovalbee(token="your_api_token", server_address="http://localhost:8000")
# or
import ovalbee as ob

ob.login("your_api_token", "http://localhost:8000")  # login to the server (in current workspace)


# *------------------------------------------------------------------------------------
# *--- Space and Collection -----------------------------------------------------------
# *------------------------------------------------------------------------------------
space = ob.space()  # get space info
space.members()  # ? list members of the space

# get collections in the space
collections = space.collections()
for collection in collections:
    print(collection)

collection = space.collection(
    "my-collection", create_ok=True
)  # get or create collection in the space
collection.delete()  # delete collection from the space
space.delete()  # delete the space


# *------------------------------------------------------------------------------------
# *--- Collection, Assets, Annotations ------------------------------------------------
# *------------------------------------------------------------------------------------
# space.upload_asset("path/to/local/image_1.jpg", "image_1") # upload local file as asset to the space
collection.add_asset("path/to/local/image_1.jpg")  # upload local file as asset to the space

asset = collection.asset("image_1.jpg")  # get asset in the collection
asset.clone(asset_id=111)
asset.delete()  # delete asset from the collection

# asset_anns = asset.annotations() # get annotations of the asset
for ann in asset.annotations():  # downloads and iterate annotations of the asset
    print(ann)
    break

asset.download("path/to/download/image_1.jpg")  # download asset to local path

asset.add_annotation(ann)  # add annotation to the asset

# *------------------------------------------------------------------------------------
# *--- Operations ---------------------------------------------------------------------
# *------------------------------------------------------------------------------------
# convert to COCO, filter classes, upload to the asset
ann.to_coco().filter_classes(["cat", "dog"]).upload(asset.id)

ann.render(img=asset.download_np())  # (install ovalbee[render])
ann.render(img=asset.download_np())  # (install ovalbee[ml-light])

new_collection = collection.transform("rotate", angle=90)  # apply transformation


# *-------------------------------------------------------------------------------------
# *--- Training ------------------------------------------------------------------------
# *-------------------------------------------------------------------------------------
# split
collection.split(train=0.7, val=0.2, test=0.1, shuffle=True)

# hyperparameters (install ovalbee[yolo] extra)
hyperparams = ob.HyperParams(model="yolov11n", epochs=25, batch_size=16, learning_rate=0.001)

# train (install ovalbee[yolo] extra)
collection.train(model="yolov11n", hyperparams=hyperparams)
