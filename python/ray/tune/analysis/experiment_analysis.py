import json
import logging
import os
import warnings
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple

from ray.util.debug import log_once
from ray.tune.utils import flatten_dict
from ray.tune.utils.serialization import TuneFunctionDecoder
from ray.tune.utils.util import is_nan_or_inf

try:
    import pandas as pd
    from pandas import DataFrame
except ImportError:
    pd = None
    DataFrame = None

from ray.tune.error import TuneError
from ray.tune.result import DEFAULT_METRIC, EXPR_PROGRESS_FILE, \
    EXPR_RESULT_FILE, EXPR_PARAM_FILE, CONFIG_PREFIX, TRAINING_ITERATION
from ray.tune.trial import Trial
from ray.tune.trial_runner import (find_newest_experiment_checkpoint,
                                   load_trials_from_experiment_checkpoint)
from ray.tune.utils.trainable import TrainableUtil
from ray.tune.utils.util import unflattened_lookup

from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

DEFAULT_FILE_TYPE = "csv"


@PublicAPI(stability="beta")
class Analysis:
    """Analyze all results from a directory of experiments.

    To use this class, the experiment must be executed with the JsonLogger.

    Args:
        experiment_dir (str): Directory of the experiment to load.
        default_metric (str): Default metric for comparing results. Can be
            overwritten with the ``metric`` parameter in the respective
            functions. If None but a mode was passed, the anonymous metric
            `ray.tune.result.DEFAULT_METRIC` will be used per default.
        default_mode (str): Default mode for comparing results. Has to be one
            of [min, max]. Can be overwritten with the ``mode`` parameter
            in the respective functions.
        file_type (str): Read results from json or csv files. Has to be one
            of [None, json, csv]. Defaults to csv.
    """

    def __init__(self,
                 experiment_dir: str,
                 default_metric: Optional[str] = None,
                 default_mode: Optional[str] = None,
                 file_type: Optional[str] = None):
        experiment_dir = os.path.expanduser(experiment_dir)
        if not os.path.isdir(experiment_dir):
            raise ValueError(
                "{} is not a valid directory.".format(experiment_dir))
        self._experiment_dir = experiment_dir
        self._configs = {}
        self._trial_dataframes = {}

        self.default_metric = default_metric
        if default_mode and default_mode not in ["min", "max"]:
            raise ValueError(
                "`default_mode` has to be None or one of [min, max]")
        self.default_mode = default_mode
        self._file_type = self._validate_filetype(file_type)

        if self.default_metric is None and self.default_mode:
            # If only a mode was passed, use anonymous metric
            self.default_metric = DEFAULT_METRIC

        if not pd:
            logger.warning(
                "pandas not installed. Run `pip install pandas` for "
                "Analysis utilities.")
        else:
            self.fetch_trial_dataframes()

    def _validate_filetype(self, file_type: Optional[str] = None):
        if file_type not in {None, "json", "csv"}:
            raise ValueError(
                "`file_type` has to be None or one of [json, csv].")
        return file_type or DEFAULT_FILE_TYPE

    def set_filetype(self, file_type: Optional[str] = None):
        """Overrides the existing file type.

        Args:
            file_type (str): Read results from json or csv files. Has to be one
                of [None, json, csv]. Defaults to csv.
        """
        self._file_type = self._validate_filetype(file_type)
        self.fetch_trial_dataframes()
        return True

    def _validate_metric(self, metric: str) -> str:
        if not metric and not self.default_metric:
            raise ValueError(
                "No `metric` has been passed and  `default_metric` has "
                "not been set. Please specify the `metric` parameter.")
        return metric or self.default_metric

    def _validate_mode(self, mode: str) -> str:
        if not mode and not self.default_mode:
            raise ValueError(
                "No `mode` has been passed and  `default_mode` has "
                "not been set. Please specify the `mode` parameter.")
        if mode and mode not in ["min", "max"]:
            raise ValueError("If set, `mode` has to be one of [min, max]")
        return mode or self.default_mode

    def dataframe(self,
                  metric: Optional[str] = None,
                  mode: Optional[str] = None) -> DataFrame:
        """Returns a pandas.DataFrame object constructed from the trials.

        This function will look through all observed results of each trial
        and return the one corresponding to the passed ``metric`` and
        ``mode``: If ``mode=min``, it returns the result with the lowest
        *ever* observed ``metric`` for this trial (this is not necessarily
        the last)! For ``mode=max``, it's the highest, respectively. If
        ``metric=None`` or ``mode=None``, the last result will be returned.

        Args:
            metric (str): Key for trial info to order on.
                If None, uses last result.
            mode (None|str): One of [None, "min", "max"].

        Returns:
            pd.DataFrame: Constructed from a result dict of each trial.
        """
        # Do not validate metric/mode here or set from default metric/mode!
        # Otherwise we will get confusing results as the lowest ever observed
        # result may not be the last result.
        if mode and mode not in ["min", "max"]:
            raise ValueError("If set, `mode` has to be one of [min, max]")

        if mode and not metric:
            raise ValueError(
                "If a `mode` is passed to `Analysis.dataframe(), you'll "
                "also have to pass a `metric`!")

        rows = self._retrieve_rows(metric=metric, mode=mode)
        all_configs = self.get_all_configs(prefix=True)
        for path, config in all_configs.items():
            if path in rows:
                rows[path].update(config)
                rows[path].update(logdir=path)
        return pd.DataFrame(list(rows.values()))

    def get_best_config(self,
                        metric: Optional[str] = None,
                        mode: Optional[str] = None) -> Optional[Dict]:
        """Retrieve the best config corresponding to the trial.

        Args:
            metric (str): Key for trial info to order on. Defaults to
                ``self.default_metric``.
            mode (str): One of [min, max]. Defaults to
                ``self.default_mode``.
        """
        metric = self._validate_metric(metric)
        mode = self._validate_mode(mode)

        rows = self._retrieve_rows(metric=metric, mode=mode)
        if not rows:
            # only nans encountered when retrieving rows
            logger.warning("Not able to retrieve the best config for {} "
                           "according to the specified metric "
                           "(only nans encountered).".format(
                               self._experiment_dir))
            return None
        all_configs = self.get_all_configs()
        compare_op = max if mode == "max" else min
        best_path = compare_op(rows, key=lambda k: rows[k][metric])
        return all_configs[best_path]

    def get_best_logdir(self,
                        metric: Optional[str] = None,
                        mode: Optional[str] = None) -> Optional[str]:
        """Retrieve the logdir corresponding to the best trial.

        Args:
            metric (str): Key for trial info to order on. Defaults to
                ``self.default_metric``.
            mode (str): One of [min, max]. Defaults to ``self.default_mode``.
        """
        metric = self._validate_metric(metric)
        mode = self._validate_mode(mode)

        assert mode in ["max", "min"]
        df = self.dataframe(metric=metric, mode=mode)
        mode_idx = pd.Series.idxmax if mode == "max" else pd.Series.idxmin
        try:
            return df.iloc[mode_idx(df[metric])].logdir
        except KeyError:
            # all dirs contains only nan values
            # for the specified metric
            # -> df is an empty dataframe
            logger.warning("Not able to retrieve the best logdir for {} "
                           "according to the specified metric "
                           "(only nans encountered).".format(
                               self._experiment_dir))
            return None

    def fetch_trial_dataframes(self) -> Dict[str, DataFrame]:
        """Fetches trial dataframes from files.

        Returns:
            A dictionary containing "trial dir" to Dataframe.
        """
        fail_count = 0
        force_dtype = {"trial_id": str}  # Never convert trial_id to float.
        for path in self._get_trial_paths():
            try:
                if self._file_type == "json":
                    with open(os.path.join(path, EXPR_RESULT_FILE), "r") as f:
                        json_list = [json.loads(line) for line in f if line]
                    df = pd.json_normalize(json_list, sep="/")
                elif self._file_type == "csv":
                    df = pd.read_csv(
                        os.path.join(path, EXPR_PROGRESS_FILE),
                        dtype=force_dtype)
                self.trial_dataframes[path] = df
            except Exception:
                fail_count += 1

        if fail_count:
            logger.debug(
                "Couldn't read results from {} paths".format(fail_count))
        return self.trial_dataframes

    def get_all_configs(self, prefix: bool = False) -> Dict[str, Dict]:
        """Returns a list of all configurations.

        Args:
            prefix (bool): If True, flattens the config dict
                and prepends `config/`.

        Returns:
            Dict[str, Dict]: Dict of all configurations of trials, indexed by
                their trial dir.
        """
        fail_count = 0
        for path in self._get_trial_paths():
            try:
                with open(os.path.join(path, EXPR_PARAM_FILE)) as f:
                    config = json.load(f)
                    if prefix:
                        for k in list(config):
                            config[CONFIG_PREFIX + k] = config.pop(k)
                    self._configs[path] = config
            except Exception:
                fail_count += 1

        if fail_count:
            logger.warning(
                "Couldn't read config from {} paths".format(fail_count))
        return self._configs

    def get_trial_checkpoints_paths(self,
                                    trial: Trial,
                                    metric: Optional[str] = None
                                    ) -> List[Tuple[str, Number]]:
        """Gets paths and metrics of all persistent checkpoints of a trial.

        Args:
            trial (Trial): The log directory of a trial, or a trial instance.
            metric (str): key for trial info to return, e.g. "mean_accuracy".
                "training_iteration" is used by default if no value was
                passed to ``self.default_metric``.

        Returns:
            List of [path, metric] for all persistent checkpoints of the trial.
        """
        metric = metric or self.default_metric or TRAINING_ITERATION

        if isinstance(trial, str):
            trial_dir = os.path.expanduser(trial)
            # Get checkpoints from logdir.
            chkpt_df = TrainableUtil.get_checkpoints_paths(trial_dir)

            # Join with trial dataframe to get metrics.
            trial_df = self.trial_dataframes[trial_dir]
            path_metric_df = chkpt_df.merge(
                trial_df, on="training_iteration", how="inner")
            return path_metric_df[["chkpt_path", metric]].values.tolist()
        elif isinstance(trial, Trial):
            checkpoints = trial.checkpoint_manager.best_checkpoints()
            # Support metrics given as paths, e.g.
            # "info/learner/default_policy/policy_loss".
            return [(c.value, unflattened_lookup(metric, c.result))
                    for c in checkpoints]
        else:
            raise ValueError("trial should be a string or a Trial instance.")

    def get_best_checkpoint(self,
                            trial: Trial,
                            metric: Optional[str] = None,
                            mode: Optional[str] = None) -> Optional[str]:
        """Gets best persistent checkpoint path of provided trial.

        Args:
            trial (Trial): The log directory of a trial, or a trial instance.
            metric (str): key of trial info to return, e.g. "mean_accuracy".
                "training_iteration" is used by default if no value was
                passed to ``self.default_metric``.
            mode (str): One of [min, max]. Defaults to ``self.default_mode``.

        Returns:
            Path for best checkpoint of trial determined by metric
        """
        metric = metric or self.default_metric or TRAINING_ITERATION
        mode = self._validate_mode(mode)

        checkpoint_paths = self.get_trial_checkpoints_paths(trial, metric)
        if not checkpoint_paths:
            logger.error(f"No checkpoints have been found for trial {trial}.")
            return None
        if mode == "max":
            return max(checkpoint_paths, key=lambda x: x[1])[0]
        else:
            return min(checkpoint_paths, key=lambda x: x[1])[0]

    def get_last_checkpoint(self,
                            trial=None,
                            metric="training_iteration",
                            mode="max"):
        """Helper function that wraps Analysis.get_best_checkpoint().
        Gets the last persistent checkpoint path of the provided trial,
        i.e., with the highest "training_iteration".
        If no trial is specified, it loads the best trial according to the
        provided metric and mode (defaults to max. training iteration).

        Args:
            trial (Trial): The log directory or an instance of a trial.
            If None, load the latest trial automatically.
            metric (str): If no trial is specified, use this metric to identify
            the best trial and load the last checkpoint from this trial.
            mode (str): If no trial is specified, use the metric and this mode
            to identify the best trial and load the last checkpoint from it.

        Returns:
            Path for last checkpoint of trial
        """
        if trial is None:
            trial = self.get_best_logdir(metric, mode)

        return self.get_best_checkpoint(trial, "training_iteration", "max")

    def _retrieve_rows(self,
                       metric: Optional[str] = None,
                       mode: Optional[str] = None) -> Dict[str, Any]:
        assert mode is None or mode in ["max", "min"]
        assert not mode or metric
        rows = {}
        for path, df in self.trial_dataframes.items():
            if mode == "max":
                idx = df[metric].idxmax()
            elif mode == "min":
                idx = df[metric].idxmin()
            else:
                idx = -1
            try:
                rows[path] = df.iloc[idx].to_dict()
            except TypeError:
                # idx is nan
                logger.warning(
                    "Warning: Non-numerical value(s) encountered for {}".
                    format(path))

        return rows

    def _get_trial_paths(self) -> List[str]:
        _trial_paths = []
        for trial_path, _, files in os.walk(self._experiment_dir):
            if (self._file_type == "json"
                and EXPR_RESULT_FILE in files) \
                    or (self._file_type == "csv"
                        and EXPR_PROGRESS_FILE in files):
                _trial_paths += [trial_path]

        if not _trial_paths:
            raise TuneError("No trials found in {}.".format(
                self._experiment_dir))
        return _trial_paths

    @property
    def trial_dataframes(self) -> Dict[str, DataFrame]:
        """List of all dataframes of the trials."""
        return self._trial_dataframes


@PublicAPI(stability="beta")
class ExperimentAnalysis(Analysis):
    """Analyze results from a Tune experiment.

    To use this class, the experiment must be executed with the JsonLogger.

    Parameters:
        experiment_checkpoint_path (str): Path to a json file
            representing an experiment state. Corresponds to
            Experiment.local_dir/Experiment.name/experiment_state.json
        trials (list|None): List of trials that can be accessed via
            `analysis.trials`.
        default_metric (str): Default metric for comparing results. Can be
            overwritten with the ``metric`` parameter in the respective
            functions.
        default_mode (str): Default mode for comparing results. Has to be one
            of [min, max]. Can be overwritten with the ``mode`` parameter
            in the respective functions.

    Example:
        >>> tune.run(my_trainable, name="my_exp", local_dir="~/tune_results")
        >>> analysis = ExperimentAnalysis(
        >>>     experiment_checkpoint_path="~/tune_results/my_exp/state.json")
    """

    def __init__(self,
                 experiment_checkpoint_path: str,
                 trials: Optional[List[Trial]] = None,
                 default_metric: Optional[str] = None,
                 default_mode: Optional[str] = None):
        experiment_checkpoint_path = os.path.expanduser(
            experiment_checkpoint_path)

        if os.path.isdir(experiment_checkpoint_path):
            # Case 1: Dir specified, find latest checkpoint.
            latest_checkpoint = find_newest_experiment_checkpoint(
                experiment_checkpoint_path)
            if not latest_checkpoint:
                raise ValueError(
                    f"The directory `{experiment_checkpoint_path}` does not "
                    f"contain a Ray Tune experiment checkpoint.")
        elif not os.path.isfile(experiment_checkpoint_path):
            # Case 2: File specified, but does not exist.
            raise ValueError(
                f"The file `{experiment_checkpoint_path}` does not "
                f"exist and cannot be loaded for experiment analysis.")
        else:
            # Case 3: File specified, use as latest checkpoint.
            latest_checkpoint = experiment_checkpoint_path

        with open(latest_checkpoint) as f:
            _experiment_state = json.load(f, cls=TuneFunctionDecoder)
            self._experiment_state = _experiment_state

        if "checkpoints" not in _experiment_state:
            raise TuneError("Experiment state invalid; no checkpoints found.")
        self._checkpoints = [
            json.loads(cp, cls=TuneFunctionDecoder)
            if isinstance(cp, str) else cp
            for cp in _experiment_state["checkpoints"]
        ]
        self.trials = trials

        # Use current dir per default
        experiment_dir = os.path.dirname(
            os.path.abspath(latest_checkpoint)) or os.getcwd()

        super(ExperimentAnalysis, self).__init__(experiment_dir,
                                                 default_metric, default_mode)

    @property
    def best_trial(self) -> Trial:
        """Get the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_trial(metric, mode, scope)` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_trial`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_trial(metric, mode)` method to set the metric "
                "and mode explicitly.")
        return self.get_best_trial(self.default_metric, self.default_mode)

    @property
    def best_config(self) -> Dict:
        """Get the config of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_config(metric, mode, scope)` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_config`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_config(metric, mode)` method to set the metric "
                "and mode explicitly.")
        return self.get_best_config(self.default_metric, self.default_mode)

    @property
    def best_checkpoint(self) -> str:
        """Get the checkpoint path of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_checkpoint(trial, metric, mode)` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_checkpoint`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_checkpoint(trial, metric, mode)` method to set the "
                "metric and mode explicitly.")
        best_trial = self.best_trial
        if not best_trial:
            raise ValueError(
                f"No best trial found. Please check if you specified the "
                f"correct default metric ({self.default_metric}) and mode "
                f"({self.default_mode}).")
        return self.get_best_checkpoint(best_trial, self.default_metric,
                                        self.default_mode)

    @property
    def best_logdir(self) -> str:
        """Get the logdir of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_logdir(metric, mode)` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_logdir`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_logdir(metric, mode, scope)` method to set the "
                "metric and mode explicitly.")
        return self.get_best_logdir(self.default_metric, self.default_mode)

    @property
    def best_dataframe(self) -> DataFrame:
        """Get the full result dataframe of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_logdir(metric, mode)` and use it to look for the dataframe
        in the `self.trial_dataframes` dict.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_result`, pass a `metric` and `mode` "
                "parameter to `tune.run()`.")
        best_logdir = self.best_logdir
        return self.trial_dataframes[best_logdir]

    @property
    def best_result(self) -> Dict:
        """Get the last result of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_trial(metric, mode, scope).last_result` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_result`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use "
                "`get_best_trial(metric, mode).last_result` to set "
                "the metric and mode explicitly and fetch the last result.")
        return self.best_trial.last_result

    def _delimiter(self):
        # Deprecate: 1.9  (default should become `/`)
        delimiter = os.environ.get("TUNE_RESULT_DELIM", ".")
        if delimiter == "." and log_once("delimiter_deprecation"):
            warnings.warn(
                "Dataframes will use '/' instead of '.' to delimit "
                "nested result keys in future versions of Ray. For forward "
                "compatibility, set the environment variable "
                "TUNE_RESULT_DELIM='/'")
        return delimiter

    @property
    def best_result_df(self) -> DataFrame:
        """Get the best result of the experiment as a pandas dataframe.

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_trial(metric, mode, scope).last_result` instead.
        """
        if not pd:
            raise ValueError("`best_result_df` requires pandas. Install with "
                             "`pip install pandas`.")

        best_result = flatten_dict(
            self.best_result, delimiter=self._delimiter())
        return pd.DataFrame.from_records([best_result], index="trial_id")

    @property
    def results(self) -> Dict[str, Dict]:
        """Get the last result of the all trials of the experiment"""
        return {trial.trial_id: trial.last_result for trial in self.trials}

    @property
    def results_df(self) -> DataFrame:
        """Get all the last results as a pandas dataframe."""
        if not pd:
            raise ValueError("`results_df` requires pandas. Install with "
                             "`pip install pandas`.")
        return pd.DataFrame.from_records(
            [
                flatten_dict(trial.last_result, delimiter=self._delimiter())
                for trial in self.trials
            ],
            index="trial_id")

    def get_best_trial(self,
                       metric: Optional[str] = None,
                       mode: Optional[str] = None,
                       scope: str = "last",
                       filter_nan_and_inf: bool = True) -> Optional[Trial]:
        """Retrieve the best trial object.

        Compares all trials' scores on ``metric``.
        If ``metric`` is not specified, ``self.default_metric`` will be used.
        If `mode` is not specified, ``self.default_mode`` will be used.
        These values are usually initialized by passing the ``metric`` and
        ``mode`` parameters to ``tune.run()``.

        Args:
            metric (str): Key for trial info to order on. Defaults to
                ``self.default_metric``.
            mode (str): One of [min, max]. Defaults to ``self.default_mode``.
            scope (str): One of [all, last, avg, last-5-avg, last-10-avg].
                If `scope=last`, only look at each trial's final step for
                `metric`, and compare across trials based on `mode=[min,max]`.
                If `scope=avg`, consider the simple average over all steps
                for `metric` and compare across trials based on
                `mode=[min,max]`. If `scope=last-5-avg` or `scope=last-10-avg`,
                consider the simple average over the last 5 or 10 steps for
                `metric` and compare across trials based on `mode=[min,max]`.
                If `scope=all`, find each trial's min/max score for `metric`
                based on `mode`, and compare trials based on `mode=[min,max]`.
            filter_nan_and_inf (bool): If True (default), NaN or infinite
                values are disregarded and these trials are never selected as
                the best trial.
        """
        metric = self._validate_metric(metric)
        mode = self._validate_mode(mode)

        if scope not in ["all", "last", "avg", "last-5-avg", "last-10-avg"]:
            raise ValueError(
                "ExperimentAnalysis: attempting to get best trial for "
                "metric {} for scope {} not in [\"all\", \"last\", \"avg\", "
                "\"last-5-avg\", \"last-10-avg\"]. "
                "If you didn't pass a `metric` parameter to `tune.run()`, "
                "you have to pass one when fetching the best trial.".format(
                    metric, scope))
        best_trial = None
        best_metric_score = None
        for trial in self.trials:
            if metric not in trial.metric_analysis:
                continue

            if scope in ["last", "avg", "last-5-avg", "last-10-avg"]:
                metric_score = trial.metric_analysis[metric][scope]
            else:
                metric_score = trial.metric_analysis[metric][mode]

            if filter_nan_and_inf and is_nan_or_inf(metric_score):
                continue

            if best_metric_score is None:
                best_metric_score = metric_score
                best_trial = trial
                continue

            if (mode == "max") and (best_metric_score < metric_score):
                best_metric_score = metric_score
                best_trial = trial
            elif (mode == "min") and (best_metric_score > metric_score):
                best_metric_score = metric_score
                best_trial = trial

        if not best_trial:
            logger.warning(
                "Could not find best trial. Did you pass the correct `metric` "
                "parameter?")
        return best_trial

    def get_best_config(self,
                        metric: Optional[str] = None,
                        mode: Optional[str] = None,
                        scope: str = "last") -> Optional[Dict]:
        """Retrieve the best config corresponding to the trial.

        Compares all trials' scores on `metric`.
        If ``metric`` is not specified, ``self.default_metric`` will be used.
        If `mode` is not specified, ``self.default_mode`` will be used.
        These values are usually initialized by passing the ``metric`` and
        ``mode`` parameters to ``tune.run()``.

        Args:
            metric (str): Key for trial info to order on. Defaults to
                ``self.default_metric``.
            mode (str): One of [min, max]. Defaults to ``self.default_mode``.
            scope (str): One of [all, last, avg, last-5-avg, last-10-avg].
                If `scope=last`, only look at each trial's final step for
                `metric`, and compare across trials based on `mode=[min,max]`.
                If `scope=avg`, consider the simple average over all steps
                for `metric` and compare across trials based on
                `mode=[min,max]`. If `scope=last-5-avg` or `scope=last-10-avg`,
                consider the simple average over the last 5 or 10 steps for
                `metric` and compare across trials based on `mode=[min,max]`.
                If `scope=all`, find each trial's min/max score for `metric`
                based on `mode`, and compare trials based on `mode=[min,max]`.
        """
        best_trial = self.get_best_trial(metric, mode, scope)
        return best_trial.config if best_trial else None

    def get_best_logdir(self,
                        metric: Optional[str] = None,
                        mode: Optional[str] = None,
                        scope: str = "last") -> Optional[str]:
        """Retrieve the logdir corresponding to the best trial.

        Compares all trials' scores on `metric`.
        If ``metric`` is not specified, ``self.default_metric`` will be used.
        If `mode` is not specified, ``self.default_mode`` will be used.
        These values are usually initialized by passing the ``metric`` and
        ``mode`` parameters to ``tune.run()``.

        Args:
            metric (str): Key for trial info to order on. Defaults to
                ``self.default_metric``.
            mode (str): One of [min, max]. Defaults to ``self.default_mode``.
            scope (str): One of [all, last, avg, last-5-avg, last-10-avg].
                If `scope=last`, only look at each trial's final step for
                `metric`, and compare across trials based on `mode=[min,max]`.
                If `scope=avg`, consider the simple average over all steps
                for `metric` and compare across trials based on
                `mode=[min,max]`. If `scope=last-5-avg` or `scope=last-10-avg`,
                consider the simple average over the last 5 or 10 steps for
                `metric` and compare across trials based on `mode=[min,max]`.
                If `scope=all`, find each trial's min/max score for `metric`
                based on `mode`, and compare trials based on `mode=[min,max]`.
        """
        best_trial = self.get_best_trial(metric, mode, scope)
        return best_trial.logdir if best_trial else None

    def stats(self) -> Dict:
        """Returns a dictionary of the statistics of the experiment."""
        return self._experiment_state.get("stats")

    def runner_data(self) -> Dict:
        """Returns a dictionary of the TrialRunner data."""
        return self._experiment_state.get("runner_data")

    def _get_trial_paths(self) -> List[str]:
        """Overwrites Analysis to only have trials of one experiment."""
        if self.trials:
            _trial_paths = [t.logdir for t in self.trials]
        else:
            logger.warning("No `self.trials`. Drawing logdirs from checkpoint "
                           "file. This may result in some information that is "
                           "out of sync, as checkpointing is periodic.")
            _trial_paths = [
                checkpoint["logdir"] for checkpoint in self._checkpoints
            ]
            try:
                self.trials = load_trials_from_experiment_checkpoint(
                    self._experiment_state, stub=True)
            except Exception as e:
                logger.warning(
                    f"Could not load trials from experiment checkpoint. "
                    f"This means your experiment checkpoint is likely faulty "
                    f"or incomplete, and you won't have access to all "
                    f"analysis methods. "
                    f"Observed error: {e}")

        if not _trial_paths:
            raise TuneError("No trials found.")
        return _trial_paths
