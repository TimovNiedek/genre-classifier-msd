from unittest.mock import MagicMock, patch

import pandas as pd

from genre_classifier.flows.train.flow import (
    eval,
    filter_top_genres,
    fix_outliers,
    get_top_genres,
    read_data,
    train,
    train_flow,
)


class TestTrainFlow:
    @patch("genre_classifier.flows.train.flow.get_file_uri")
    @patch("genre_classifier.flows.train.flow.read_parquet_data")
    @patch("mlflow.data.from_pandas")
    @patch("mlflow.log_input")
    def test_read_data(
        self,
        mock_log_input,
        mock_from_pandas,
        mock_read_parquet_data,
        mock_get_file_uri,
    ):
        mock_get_file_uri.return_value = "s3://bucket/data.parquet"
        mock_read_parquet_data.return_value = pd.DataFrame(
            {"col1": [1, 2], "col2": [3, 4]}
        )
        mock_from_pandas.return_value = MagicMock()

        data = read_data("data/train.parquet", "bucket")
        assert data.equals(pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}))

    def test_get_top_genres(self):
        df = pd.DataFrame(
            {"genres": [["rock", "pop"], ["jazz"], ["rock"], ["pop", "jazz"]]}
        )
        top_genres = get_top_genres(df, k=2)
        assert top_genres == ["rock", "pop"]

    def test_filter_top_genres(self):
        df = pd.DataFrame(
            {"genres": [["rock", "pop"], ["jazz"], ["rock"], ["pop", "jazz"]]}
        )
        top_genres = ["rock", "pop"]
        filtered_df = filter_top_genres(df, top_genres)
        assert "genres_filtered" not in filtered_df.columns
        assert "genres" in filtered_df.columns
        assert len(filtered_df) == 3

    @patch("genre_classifier.flows.train.flow._fix_outliers")
    def test_fix_outliers(self, mock_fix_outliers):
        df = pd.DataFrame({"tempo": [60, 400, 90, 50]})
        mock_fix_outliers.return_value = df
        result_df = fix_outliers(df)
        mock_fix_outliers.assert_called_once_with(df, 70, 180)
        assert not result_df.empty

    @patch("mlflow.sklearn.log_model")
    @patch("mlflow.log_metric")
    def test_train(self, mock_log_metric, mock_log_model):
        df = pd.DataFrame(
            {
                "duration": [1, 2],
                "key": [1, 2],
                "loudness": [1, 2],
                "mode": [1, 2],
                "tempo": [1, 2],
                "year": [1, 2],
                "genres": [["rock"], ["pop"]],
            }
        )
        top_genres = ["rock", "pop"]
        pipeline, mlb = train(df, top_genres)
        assert pipeline is not None
        assert mlb is not None
        mock_log_model.assert_called()
        mock_log_metric.assert_called()

    @patch("mlflow.log_metric")
    @patch("mlflow.register_model")
    @patch("mlflow.active_run")
    def test_eval(self, mock_active_run, mock_register_model, mock_log_metric):
        df = pd.DataFrame(
            {
                "duration": [1, 2],
                "key": [1, 2],
                "loudness": [1, 2],
                "mode": [1, 2],
                "tempo": [1, 2],
                "year": [1, 2],
                "genres": [["rock"], ["pop"]],
            }
        )
        pipeline = MagicMock()
        mlb = MagicMock()
        mlb.transform.return_value = [[1, 0], [0, 1]]
        pipeline.predict.return_value = [[1, 0], [0, 1]]
        result = eval(df, pipeline, mlb, True, 0.0, 1.0, True)
        assert result
        mock_log_metric.assert_called()
        mock_register_model.assert_called()

    @patch("genre_classifier.flows.train.flow.set_aws_credential_env")
    @patch("genre_classifier.flows.train.flow.read_data")
    @patch("genre_classifier.flows.train.flow.get_top_genres")
    @patch("genre_classifier.flows.train.flow.filter_top_genres")
    @patch("genre_classifier.flows.train.flow.fix_outliers")
    @patch("genre_classifier.flows.train.flow.train")
    @patch("genre_classifier.flows.train.flow.eval")
    @patch("mlflow.set_experiment")
    @patch("mlflow.set_tracking_uri")
    @patch("mlflow.log_params")
    @patch("mlflow.start_run")
    @patch("mlflow.end_run")
    def test_train_flow(
        self,
        mock_end_run,
        mock_start_run,
        mock_log_params,
        mock_set_tracking_uri,
        mock_set_experiment,
        mock_eval,
        mock_train,
        mock_fix_outliers,
        mock_filter_top_genres,
        mock_get_top_genres,
        mock_read_data,
        mock_set_aws_credential_env,
    ):
        mock_read_data.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        mock_get_top_genres.return_value = ["rock", "pop"]
        mock_filter_top_genres.return_value = pd.DataFrame(
            {"col1": [1, 2], "col2": [3, 4]}
        )
        mock_fix_outliers.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        mock_train.return_value = (MagicMock(), MagicMock())
        mock_eval.return_value = True

        train_flow("test_experiment")
        mock_set_aws_credential_env.assert_called_once()
        mock_read_data.assert_called()
        mock_get_top_genres.assert_called_once()
        mock_filter_top_genres.assert_called()
        mock_fix_outliers.assert_called()
        mock_train.assert_called_once()
        mock_eval.assert_called_once()
        mock_set_experiment.assert_called_once_with("test_experiment")
        mock_set_tracking_uri.assert_called_once_with("http://127.0.0.1:5000")
        mock_log_params.assert_called()
        mock_start_run.assert_called()
        mock_end_run.assert_called()
