from opencensus.stats import measure as measure_module
from opencensus.stats import aggregation as aggregation_module
from opencensus.stats import view as view_module
from opencensus.stats import stats as stats_module
from opencensus.tags import TagMap, TagKey, TagValue

measurements = {}
measurement_map = stats_module.stats.stats_recorder.new_measurement_map()
tag_map = TagMap()


def initialize_tags(tags: dict):
    for tag, value in tags.items():
        tag_map.insert(TagKey(tag), TagValue(value))


def get_tag_keys():
    return list(tag_map.map.keys())


def define_measurements(measurements_config: dict):
    global measurements
    for measurement, config in measurements_config.items():
        if config["type"] == "float":
            measurements[measurement] = measure_module.MeasureFloat(
                measurement, config["description"], config["unit"]
            )
        elif config["type"] == "int":
            measurements[measurement] = measure_module.MeasureInt(
                measurement, config["description"], config["unit"]
            )
        else:
            raise ValueError("Unknown measurement type")


def define_views(views_config: dict):
    global measurements
    for view, config in views_config.items():
        if config["aggregation"] == "sum":
            aggr = aggregation_module.SumAggregation()
        elif config["aggregation"] == "last_value":
            aggr = aggregation_module.LastValueAggregation()
        else:
            raise ValueError("Unknown aggregation type")

        v = view_module.View(
            view, config["description"], get_tag_keys(), measurements[view], aggr
        )
        stats_module.stats.view_manager.register_view(v)


def define_measurements_and_views(config: dict):
    define_measurements(config)
    define_views(config)


def record():
    measurement_map.record(tag_map)


def put(measurement, value):
    if isinstance(measurements[measurement], measure_module.MeasureInt):
        measurement_map.measure_int_put(measurements[measurement], value)
    elif isinstance(measurements[measurement], measure_module.MeasureFloat):
        measurement_map.measure_float_put(measurements[measurement], value)
    else:
        raise ValueError("Unknown measurement type")
