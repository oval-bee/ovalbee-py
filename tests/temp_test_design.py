# import ovalbee
# some logic to get API token and server address
# ob = ovalbee.Ovalbee(token="your_api_token", server_address="http://localhost:8000")
# or
import ovalbee as ob

api = ob.login(server_address="http://localhost:8000", token="your_api_token")
# or
api = ob.Api(server_address="http://localhost:8000", token="your_api_token")
api = ob.Api.from_env()  # get server_address and token from environment variables


# !------------------------------------------------------------------------------------
# !--- API ----------------------------------------------------------------------------
# !------------------------------------------------------------------------------------

# *--- Space --------------------------------------------------------------------------
api.space.get("test")  # get space info
api.space.list()  # list spaces
api.space.create("test")  # create space
api.space.delete(id=123)  # delete space by id

# members of the space
members = api.space.members(space_id=123)  # list members of the space
# members = space.members()  # alias for api.space.members(space_id=space.id)

space = api.space.get("test")  # for further operations

# *--- Collection ---------------------------------------------------------------------
api.collection.get(space_id=space.id, name="test")  # get collection info
api.collection.list(space_id=space.id)  # list collections
api.collection.create(space_id=space.id, name="test")  # create collection
api.collection.delete(id=123)  # delete collection by id
api.collection.clone(id=123, space_id=456)  # clone collection by id (space_id is optional)


collections = space.collections()  # alias for api.collection.list(space_id=space.id)

# alias for api.collection.get(space_id=space.id, name="my-collection")
collection = space.collection("my-collection")

# alias for api.collection.create(space_id=space.id, name="my-collection")
collection = space.collection("my-collection", create=True)


# *--- Assets -------------------------------------------------------------------------
api.asset.get(asset_id=456)  # get asset by id
api.asset.get(collection_id=123, name="image_1.jpg")  # get asset by name in the collection
api.asset.list(collection_id=123)  # list assets in the collection
api.asset.list_by_ids(asset_ids=[456, 789])  # list assets by ids
api.asset.delete(asset_id=456)  # delete asset by id
api.asset.clone(asset_id=456, collection_id=789)  # clone asset


# alias for api.asset.get(collection_id=collection.id, name="image_1.jpg")
asset = collection.asset("image_1.jpg")

# alias for api.asset.list(collection_id=collection.id)
assets = collection.assets()

# download and upload
asset.download("path/to/download/image_1.jpg")  # download asset to local path
api.asset.create(collection_id=123, name="image_1.jpg", file_path="path/to/local/image_1.jpg")
api.asset.upload(collection_id=123, name="image_1.jpg", file_path="path/to/local/image_1.jpg")
# alias:
collection.upload_asset(file_path="path/to/local/image_1.jpg")


# *--- Annotation ---------------------------------------------------------------------
api.annotation.get(annotation_id=789)  # get annotation by id
api.annotation.list(asset_id=456)  # list annotations for the asset
api.annotation.delete(annotation_id=789)  # delete annotation by id
api.annotation.clone(annotation_id=789, asset_id=1011)  # clone annotation (asset_id is optional)

# alias for api.annotation.list(asset_id=asset.id)
annotations = asset.annotations()
for ann in annotations:
    pass


# upload annotation
api.annotation.add(asset_id=456, ann=ann)  # append annotation to the asset
ann.upload(asset_id=456)  # upload annotation to the asset
asset.add_annotation(ann)  # add annotation to the asset
asset.replace_annotations(ann)


# !------------------------------------------------------------------------------------
# !--- Operations ---------------------------------------------------------------------
# !------------------------------------------------------------------------------------

# ? collection.download(path="path/to/download")  # ? structure??? do we need to have specific local structure?
# ? maybe we can have only collection.download_assets(path="path/to/download") > just a folder with assets

# ? collection.download_annotations(format="coco", path="path/to/download")  # download annotations in COCO format

# convert to COCO, filter classes, upload to the asset
ann.to_coco().filter_classes(["cat", "dog"]).upload(asset.id)
# or
ann.to_coco(inplace=True)
ann.filter_classes(["cat", "dog"], inplace=True)
ann.upload(inplace=True)

ann.render(img=asset.download_np())  # (install ovalbee[render])

collection.transform("rotate", angle=90)
collection.split(train=0.7, val=0.2, test=0.1, shuffle=True)


# *-------------------------------------------------------------------------------------
# *--- Training ------------------------------------------------------------------------
# *-------------------------------------------------------------------------------------
# hyperparameters (install ovalbee[yolo] extra)
hyperparams = ob.HyperParams(model="yolov11n", epochs=25, batch_size=16, learning_rate=0.001)


# train (install ovalbee[yolo] extra)
api.nn.list_trainings(collection_id=collection.id)
api.nn.train(collection_id=collection.id, model="yolov11n", hyperparams=hyperparams)
train_info = collection.train(model="yolov11n", hyperparams=hyperparams)
# monitor training
api.nn.train_status(training_id=train_info.id)
# or
api.nn.wait_for_training_completion(training_id=train_info.id)

# train info
train_info = api.nn.get_training_info(training_id=train_info.id)

# artifacts:
# - trained model
# - training logs
# - evaluation metrics
artifacts = api.collection.list_training_artifact(collection_id=collection.id)
collection.training_artifacts()

api.nn.get_training_artifact(training_id=train_info.id)
api.collection.get_training_artifact(training_id=train_info.id)
collection.training_artifact(training_id=train_info.id)

# evaluate trained model on another collection
eval_results = api.nn.evaluate(training_id=train_info.id, collection_id=collection.id)
eval_results = api.nn.evaluate(model_path="path/to/model.pt", collection_id=collection.id)
eval_results = api.nn.evaluate(artifacts_id=artifacts.id, collection_id=collection.id)
eval_results = api.nn.evaluate(artifacts_path="path/to/artifacts", collection_id=collection.id)
eval_results = collection.evaluate(model_path="path/to/model.pt")
eval_results = collection.evaluate(artifacts_id=artifacts.id)
eval_results = collection.evaluate(artifacts_path="path/to/artifacts")

# deploy trained model as a service
deployment_info = api.nn.deploy(training_id=train_info.id)
deployment_info = api.nn.deploy(model_path="path/to/model.pt")
deployment_info = api.nn.deploy(artifacts_id=artifacts.id)
deployment_info = api.nn.deploy(artifacts_path="path/to/artifacts")

# compare models
comparison = api.nn.compare_models(collection.id, [deployment_info, deployment_info])

comparison = api.nn.compare_models(collection.id, ["model_a.pt", "model_b.pt"])
comparison = api.nn.compare_models([artifacts.id, "path/to/model_b.pt"], collection.id)

comparison = collection.compare_models(["path/to/model_a.pt", "path/to/model_b.pt"])
comparison = collection.compare_models([artifacts.id, "path/to/artifacts"])

# -------------------------------------------------------------------------------------
# --- Complete Workflow Example -------------------------------------------------------
# -------------------------------------------------------------------------------------

# 1. Login to Ovalbee API
api = ob.login(server_address="http://localhost:8000", token="your_api_token")

# 2. Access a space and collection
space = api.space.get("my-space")
collection = space.collection("my-collection")

# 3. List assets and their annotations
assets = collection.assets()
for asset in assets:
    print(f"Asset: {asset.name}")
    annotations = asset.annotations()
    for ann in annotations:
        print(f" - Annotation ID: {ann.id}")
        print(f" - Annotation Format: {ann.format}")

# 4. Train a model on the collection
hyperparams = ob.YoloHyperParams(epochs=25, batch_size=16, learning_rate=0.001)
train_info = collection.train(model="yolov11n", hyperparams=hyperparams)

# 5. Monitor training status and get artifacts
api.nn.wait_for_training_completion(training_id=train_info.id)

# 6. Evaluate the trained model on a validation collection
val_collection = space.collection("my-validation-collection")
eval_results = val_collection.evaluate(model_path="path/to/trained/model.pt")
print(f"Evaluation Results: {eval_results}")

# 7. Compare multiple trained models
comparison = collection.compare_models([artifacts.id, "path/to/another/model.pt"])

# 8. Deploy the best model as a service
deployment_info = api.nn.deploy(artifacts_id=comparison.best_model)

# 9. Predict using the deployed model
api.nn.predict(model=deployment_info.id, src="path/to/input", dst="path/to/predictions")

# 10 Visualize predictions
collection.download_assets(path="path/to/predictions")
annotations = ob.Annotation.load_from_folder("path/to/predictions")
ob.render_annotations(imgs="path/to/predictions", anns=annotations, dst="path/to/visualizations")
