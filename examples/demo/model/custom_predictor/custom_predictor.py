import os

import lightgbm as lgb


class LGBMPredictor(object):
    """A Predictor for an AI Platform custom prediction routine."""

    def __init__(self, model):
        """Stores artifacts for prediction. Only initialized via `from_path`."""
        self._model = model

    def predict(self, instances, **kwargs):
        """Performs custom prediction.

        Performs prediction using the trained LightGBM model.

        Args:
            instances: A list of prediction input instances.
            **kwargs: A dictionary of keyword args provided as additional
                fields on the predict request body.

        Returns:
            A list of outputs containing the prediction results.
        """
        outputs = self._model.predict(instances)
        return outputs.tolist()

    @classmethod
    def from_path(cls, model_dir):
        """Creates an instance of LGBMPredictor using the given path.

        This loads artifacts that have been copied from your model directory (model_dir) in
        Cloud Storage. LGBMPredictor uses them during prediction.

        Args:
            model_dir: The directory that contains the trained model

        Returns:
            An instance of `LGBMPredictor`.
        """
        model_path = os.path.join(model_dir, 'lgbm.txt')
        bst = lgb.Booster(model_file=model_path)

        return cls(bst)
