from splunklib.client import connect
from splunklib.results import ResultsReader
from splunktospan import LogParser, DictParser
import datetime

try:
    import utils
except ImportError:
    raise Exception("Add the SDK repository to your PYTHONPATH to run the examples "
                    "(e.g., export PYTHONPATH=~/splunk-sdk-python.")

# for examples of tailing off the splunk API see
# https://github.com/splunk/splunk-sdk-python/blob/master/examples/stail.py
# and https://github.com/splunk/splunk-sdk-python/blob/master/examples/follow.py

def example_main():
    duration_keys = ["latencyMillis", "elapsedMillis", "duration", 'dur']
    ls_component_key = "lightstep.component_name"
    join_guid = "guid:correlation_id"
    tags_to_rewrite = {
        "correlation-id": join_guid,
        "cid": join_guid,
        "component": ls_component_key,
        "host": ls_component_key}


    opts = utils.parse(sys.argv[1:], {}, ".splunkrc", usage=usage)
    if len(opts.args) != 1:
        utils.error("Search expression required", 2)
    search = opts.args[0]
    service = connect(**opts.kwargs)

    regex = re.compile("(?P<start_time>.+) (?P<component>.+) (?P<operation>.+): (?P<tags>.*)")

    log_parser = LogParser(regex)
    log_parser.duration_keys = duration_keys
    log_parser.downcase_keys = True

    dict_parser = DictParser()
    dict_parser.downcase_keys = True
    dict_parser.operation_keys = ['activity']
    dict_parser.duration_keys = duration_keys

    result = service.get(
            "search/jobs/export",
            search=search,
            earliest_time="rt",
            latest_time="rt",
            search_mode="realtime")

    for result in results.ResultsReader(result.body):
        try:
            log = None
            # I'm not sure what will actually be used here since ResultReader can return
            # either a dictionary or message. I imagine a dict=>ParsedLog should be
            # straightforward to write.
            # https://github.com/splunk/splunk-sdk-python/blob/master/splunklib/results.py#L170
            if result is None:
                break
            if isinstance(result, dict):
                parsed = dict_parser.parse_dict(result)
            else:
                parsed = log_parser.parse_line(result.message)

            parsed.rewrite_tags(tags_to_rewrite)
            if int(parsed.tags["status"]) >= 300:
                parsed.tags["error"] = True
            parsed.to_span()
        except Exception as e:
            print("Did not parse line: ", result, "(", e, ")")
